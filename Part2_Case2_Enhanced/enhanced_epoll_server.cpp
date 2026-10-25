#include "enhanced_epoll_server.h"
#include "concurrency.h"
#include "directory_mapper.h"
#include "fault.h"
#include "metrics_logger.h"
#include "protocol.h"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <random>
#include <sys/epoll.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <unistd.h>

/*
 * enhanced_epoll_server.cpp - Member 5 (PR #6, Wave 3)
 *
 * Implements:
 *   1. Non-blocking socket configuration and Linux epoll instance
 * initialization
 *   2. Zero-copy sendfile() chunk transmission with build_file_data_header()
 *   3. Dynamic 64 KiB chunk work-queue with 3-state lifecycle (UNASSIGNED ->
 * IN_FLIGHT -> COMMITTED)
 *   4. Sub-stream fault isolation and atomic chunk requeueing
 *   5. Non-blocking epoll event loop
 */

static std::unordered_map<std::string, EnhancedSessionState *>
    g_enhanced_sessions;
static pthread_mutex_t g_enhanced_mutex = PTHREAD_MUTEX_INITIALIZER;

int set_socket_nonblocking(int sockfd) {
  int flags = fcntl(sockfd, F_GETFL, 0);
  if (flags < 0) {
    log_event("ERROR", "fcntl(F_GETFL) failed: %s", strerror(errno));
    return -1;
  }
  if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) < 0) {
    log_event("ERROR", "fcntl(F_SETFL, O_NONBLOCK) failed: %s",
              strerror(errno));
    return -1;
  }
  return 0;
}

int init_epoll_instance(int listen_fd) {
  if (set_socket_nonblocking(listen_fd) < 0) {
    log_event("ERROR", "set_socket_nonblocking failed on listen_fd=%d",
              listen_fd);
    return -1;
  }
  // Pre-tune listen socket send/recv buffers (256 KiB each)
  int buf_size = 256 * 1024;
  setsockopt(listen_fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
  setsockopt(listen_fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    log_event("ERROR", "epoll_create1 failed: %s", strerror(errno));
    return -1;
  }

  struct epoll_event ev;
  std::memset(&ev, 0, sizeof(ev));
  ev.events = EPOLLIN;
  ev.data.fd = listen_fd;

  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) {
    log_event("ERROR", "epoll_ctl(EPOLL_CTL_ADD, listen_fd) failed: %s",
              strerror(errno));
    close(epoll_fd);
    return -1;
  }

  log_event("INFO",
            "Initialized epoll instance (fd=%d) monitoring listen_fd=%d",
            epoll_fd, listen_fd);
  return epoll_fd;
}

ssize_t send_chunk_zerocopy(int client_fd, int file_fd,
                            const ChunkWorkUnit &unit, uint64_t *wire_counter) {
  uint8_t hdr[FILE_DATA_HEADER_SIZE]; // Exactly 24 bytes
  build_file_data_header(hdr, unit.file_index, unit.file_offset,
                         unit.chunk_len);

  if (send_all(client_fd, hdr, sizeof(hdr), wire_counter) < 0) {
    log_event("ERROR",
              "send_chunk_zerocopy: failed to send 24-byte header for unit %u",
              unit.unit_id);
    return -1;
  }

  off_t off = static_cast<off_t>(unit.file_offset);
  size_t remaining = unit.chunk_len;

  while (remaining > 0) {
    ssize_t sent = sendfile(client_fd, file_fd, &off, remaining);
    if (sent < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        struct pollfd pfd;
        pfd.fd = client_fd;
        pfd.events = POLLOUT;
        int prc = poll(&pfd, 1, 100);
        if (prc <= 0) {
          return -1;
        }
        continue;
      }
      if (errno == EINTR) {
        continue;
      }
      log_event("ERROR", "sendfile failed on unit %u: %s", unit.unit_id,
                strerror(errno));
      return -1;
    }
    if (sent == 0) {
      break;
    }
    remaining -= static_cast<size_t>(sent);
    if (wire_counter) {
      *wire_counter += static_cast<uint64_t>(sent);
    }
  }

  return static_cast<ssize_t>(unit.chunk_len);
}

EnhancedSessionState *create_enhanced_session(const TopicEntry &entry,
                                              uint16_t num_streams,
                                              const uint8_t session_id[16]) {
  EnhancedSessionState *sess = new EnhancedSessionState();
  std::memcpy(sess->session_id, session_id, 16);
  sess->topic = entry.topic_name;
  sess->num_streams = (num_streams > 0) ? num_streams : DEFAULT_PARALLEL_CONN;
  sess->last_active = time(nullptr);
  pthread_mutex_init(&sess->queue_mutex, nullptr);

  uint32_t u_id = 0;
  for (size_t f_idx = 0; f_idx < entry.files.size(); ++f_idx) {
    uint64_t fsize = entry.files[f_idx].file_size;
    uint64_t off = 0;
    if (fsize == 0) {
      // Edge case: empty file
      ChunkWorkUnit u;
      u.unit_id = u_id++;
      u.file_index = static_cast<uint32_t>(f_idx);
      u.file_offset = 0;
      u.chunk_len = 0;
      u.state = CHUNK_UNASSIGNED;
      u.assigned_conn_id = -1;
      sess->units.push_back(u);
    }
    while (off < fsize) {
      uint32_t clen =
          static_cast<uint32_t>(std::min<uint64_t>(CHUNK_SIZE, fsize - off));
      ChunkWorkUnit u;
      u.unit_id = u_id++;
      u.file_index = static_cast<uint32_t>(f_idx);
      u.file_offset = off;
      u.chunk_len = clen;
      u.state = CHUNK_UNASSIGNED;
      u.assigned_conn_id = -1;
      sess->units.push_back(u);
      off += clen;
    }
  }

  char hex32[33];
  session_id_to_hex(session_id, hex32);

  pthread_mutex_lock(&g_enhanced_mutex);
  g_enhanced_sessions[std::string(hex32)] = sess;
  pthread_mutex_unlock(&g_enhanced_mutex);

  log_event("INFO",
            "Created EnhancedSessionState '%s' with %u 64 KiB work units "
            "across %zu files",
            hex32, u_id, entry.files.size());
  return sess;
}

EnhancedSessionState *find_enhanced_session(const uint8_t session_id[16]) {
  char hex32[33];
  session_id_to_hex(session_id, hex32);
  pthread_mutex_lock(&g_enhanced_mutex);
  auto it = g_enhanced_sessions.find(std::string(hex32));
  EnhancedSessionState *ptr =
      (it != g_enhanced_sessions.end()) ? it->second : nullptr;
  pthread_mutex_unlock(&g_enhanced_mutex);
  return ptr;
}

void free_enhanced_session(const uint8_t session_id[16]) {
  char hex32[33];
  session_id_to_hex(session_id, hex32);
  pthread_mutex_lock(&g_enhanced_mutex);
  auto it = g_enhanced_sessions.find(std::string(hex32));
  if (it != g_enhanced_sessions.end()) {
    EnhancedSessionState *sess = it->second;
    g_enhanced_sessions.erase(it);
    pthread_mutex_destroy(&sess->queue_mutex);
    delete sess;
  }
  pthread_mutex_unlock(&g_enhanced_mutex);
}

bool pop_next_chunk_unit(EnhancedSessionState *sess, uint32_t conn_id,
                         ChunkWorkUnit *out_unit) {
  if (!sess || !out_unit)
    return false;
  pthread_mutex_lock(&sess->queue_mutex);
  sess->last_active = time(nullptr);

  for (auto &u : sess->units) {
    if (u.state == CHUNK_UNASSIGNED) {
      u.state = CHUNK_IN_FLIGHT;
      u.assigned_conn_id = static_cast<int32_t>(conn_id);
      *out_unit = u;
      pthread_mutex_unlock(&sess->queue_mutex);
      return true;
    }
  }

  pthread_mutex_unlock(&sess->queue_mutex);
  return false;
}

void requeue_stream_chunks(EnhancedSessionState *sess, int32_t conn_id) {
  if (!sess)
    return;
  pthread_mutex_lock(&sess->queue_mutex);
  uint32_t requeued = 0;
  for (auto &u : sess->units) {
    if (u.assigned_conn_id == conn_id && u.state == CHUNK_IN_FLIGHT) {
      u.state = CHUNK_UNASSIGNED;
      u.assigned_conn_id = -1;
      requeued++;
    }
  }
  pthread_mutex_unlock(&sess->queue_mutex);
  if (requeued > 0) {
    log_event("INFO", "Requeued %u in-flight chunks from dropped sub-stream %d",
              requeued, conn_id);
  }
}

void apply_client_bitmap(EnhancedSessionState *sess, const uint8_t *bitmap,
                         uint32_t bmap_len) {
  if (!sess || !bitmap || bmap_len == 0)
    return;
  pthread_mutex_lock(&sess->queue_mutex);
  uint32_t updated = 0;
  for (size_t i = 0; i < sess->units.size(); ++i) {
    if (i < bmap_len * 8) {
      if ((bitmap[i / 8] & (1 << (i % 8))) != 0) {
        if (sess->units[i].state != CHUNK_COMMITTED) {
          sess->units[i].state = CHUNK_COMMITTED;
          updated++;
        }
      }
    }
  }
  pthread_mutex_unlock(&sess->queue_mutex);
  if (updated > 0) {
    log_event("INFO",
              "Applied client bitmask: confirmed %u units committed on disk",
              updated);
  }
}

static int serve_substream_session(int client_fd, const TopicEntry &entry,
                                   EnhancedSessionState *sess, uint32_t conn_id,
                                   double pf, uint64_t *wire_counter) {
  ChunkWorkUnit unit;
  while (g_keep_running) {
    if (pop_next_chunk_unit(sess, conn_id, &unit)) {
      if (unit.file_index >= entry.files.size()) {
        continue;
      }

      int ffd = open(entry.files[unit.file_index].full_path.c_str(), O_RDONLY);
      if (ffd < 0) {
        log_event("ERROR", "Failed to open file '%s': %s",
                  entry.files[unit.file_index].full_path.c_str(),
                  strerror(errno));
        requeue_stream_chunks(sess, static_cast<int32_t>(conn_id));
        return -1;
      }

      ssize_t sent = send_chunk_zerocopy(client_fd, ffd, unit, wire_counter);
      close(ffd);

      if (sent < 0) {
        log_event("INFO", "Sub-stream %u socket error during zerocopy send",
                  conn_id);
        requeue_stream_chunks(sess, static_cast<int32_t>(conn_id));
        return -1;
      }

      // Post-chunk fault injection check
      if (should_inject_fault(pf, sess->topic.c_str(), unit.file_index,
                              unit.file_offset + unit.chunk_len)) {
        log_event(
            "FAULT",
            "Sub-stream %u dropped by fault injector after unit %u (pf=%.3f)",
            conn_id, unit.unit_id, pf);
        requeue_stream_chunks(sess, static_cast<int32_t>(conn_id));
        return -1;
      }

      // Mark committed
      pthread_mutex_lock(&sess->queue_mutex);
      sess->units[unit.unit_id].state = CHUNK_COMMITTED;
      pthread_mutex_unlock(&sess->queue_mutex);
    } else {
      bool any_in_flight = false;
      bool any_unassigned = false;
      pthread_mutex_lock(&sess->queue_mutex);
      for (const auto &u : sess->units) {
        if (u.state == CHUNK_IN_FLIGHT)
          any_in_flight = true;
        else if (u.state == CHUNK_UNASSIGNED)
          any_unassigned = true;
      }
      pthread_mutex_unlock(&sess->queue_mutex);

      if (any_unassigned) {
        continue;
      }
      if (any_in_flight) {
        usleep(15000); // 15ms wait for peer streams or fault requeue
        continue;
      }
      break; // All units are committed!
    }
  }

  // Check if all units are committed
  bool all_done = true;
  pthread_mutex_lock(&sess->queue_mutex);
  for (const auto &u : sess->units) {
    if (u.state != CHUNK_COMMITTED) {
      all_done = false;
      break;
    }
  }
  pthread_mutex_unlock(&sess->queue_mutex);

  if (all_done) {
    send_transfer_end(client_fd, static_cast<uint32_t>(entry.files.size()),
                      entry.total_bytes, wire_counter);
    log_event("INFO",
              "Sub-stream %u completed topic '%s' (%zu files, %llu bytes)",
              conn_id, entry.topic_name.c_str(), entry.files.size(),
              static_cast<unsigned long long>(entry.total_bytes));
  }
  return 0;
}

int run_enhanced_epoll_server_loop(
    int listen_fd, const std::unordered_map<std::string, TopicEntry> &topic_map,
    double pf, uint32_t seed) {
  init_thread_rng(seed);
  log_event("INFO", "Starting Case 2 Enhanced epoll event loop on listen_fd=%d",
            listen_fd);

  int epoll_fd = init_epoll_instance(listen_fd);
  if (epoll_fd < 0) {
    return -1;
  }

  const int MAX_EVENTS = 64;
  struct epoll_event events[MAX_EVENTS];

  while (g_keep_running) {
    int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 250);
    if (nfds < 0) {
      if (errno == EINTR)
        continue;
      log_event("ERROR", "epoll_wait failed: %s", strerror(errno));
      break;
    }

    for (int i = 0; i < nfds; ++i) {
      int event_fd = events[i].data.fd;

      if (event_fd == listen_fd) {
        // Accept incoming client connections
        while (true) {
          struct sockaddr_in client_addr;
          socklen_t addr_len = sizeof(client_addr);
          int client_fd = accept(
              listen_fd, reinterpret_cast<struct sockaddr *>(&client_addr),
              &addr_len);
          if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
              break;
            }
            log_event("ERROR", "accept() error: %s", strerror(errno));
            break;
          }

          char ip_buf[INET_ADDRSTRLEN];
          inet_ntop(AF_INET, &(client_addr.sin_addr), ip_buf, INET_ADDRSTRLEN);
          log_event("INFO", "Accepted connection from %s:%u (fd=%d)", ip_buf,
                    ntohs(client_addr.sin_port), client_fd);

          // Disable Nagle's algorithm + tune socket buffers to 256 KiB
          int nodelay = 1;
          setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay,
                     sizeof(nodelay));
          int buf_sz = 256 * 1024;
          setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &buf_sz, sizeof(buf_sz));
          setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, &buf_sz, sizeof(buf_sz));

          // Dispatch client connection in worker thread
          WorkerArgs *args = new WorkerArgs();
          args->client_fd = client_fd;
          args->topic_map = &topic_map;
          args->pf = pf;
          args->case_mode = 3;
          args->rng_seed = seed;

          pthread_t tid;
          pthread_attr_t attr;
          pthread_attr_init(&attr);
          pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
          if (pthread_create(&tid, &attr, worker_thread, args) != 0) {
            log_event("ERROR", "pthread_create failed: %s", strerror(errno));
            close(client_fd);
            delete args;
          }
          pthread_attr_destroy(&attr);
        }
      }
    }
  }

  close(epoll_fd);
  log_event("INFO", "Exited Case 2 Enhanced epoll event loop");
  return 0;
}

// Hook called by concurrency.cpp process_client for Enhanced opcodes 0x0C and
// 0x0D
int process_enhanced_opcodes(
    int client_fd, const FrameHeader &hdr, const std::vector<uint8_t> &payload,
    const std::unordered_map<std::string, TopicEntry> &topic_map, double pf,
    uint64_t *wire_counter) {
  if (hdr.type == OP_GET_ENHANCED) {
    MsgGetEnhanced req;
    if (!parse_get_enhanced(payload.data(),
                            static_cast<uint32_t>(payload.size()), &req)) {
      log_event("ERROR", "Malformed OP_GET_ENHANCED received on fd=%d",
                client_fd);
      send_error(client_fd, ERR_BAD_REQUEST, "Malformed OP_GET_ENHANCED",
                 wire_counter);
      return -1;
    }

    log_event("INFO",
              "Client requested OP_GET_ENHANCED topic '%s' with %u streams",
              req.topic.c_str(), req.num_streams);
    const TopicEntry *entry = get_topic_path(topic_map, req.topic);
    if (!entry) {
      log_event("ERROR", "Topic '%s' not found", req.topic.c_str());
      send_error(client_fd, ERR_TOPIC_NOT_FOUND,
                 "Topic not found: " + req.topic, wire_counter);
      return -1;
    }

    uint8_t sess_id[16];
    bool got_random = false;
    int rfd = open("/dev/urandom", O_RDONLY);
    if (rfd >= 0) {
      if (read(rfd, sess_id, 16) == 16)
        got_random = true;
      close(rfd);
    }
    if (!got_random) {
      std::random_device rd;
      for (int k = 0; k < 16; ++k)
        sess_id[k] = static_cast<uint8_t>(rd() & 0xFF);
    }

    EnhancedSessionState *sess =
        create_enhanced_session(*entry, req.num_streams, sess_id);
    (void)sess;

    // 1. Send OP_GET_OK
    send_get_ok(client_fd, sess_id, static_cast<uint32_t>(entry->files.size()),
                entry->total_bytes, wire_counter);

    // 2. Send OP_FILE_BEGIN for every file so client can pre-allocate files
    // with posix_fallocate
    for (size_t f_idx = 0; f_idx < entry->files.size(); ++f_idx) {
      const FileMeta &fm = entry->files[f_idx];
      send_file_begin(client_fd, static_cast<uint32_t>(f_idx), 0, fm.file_size,
                      fm.filename, wire_counter);
    }

    log_event(
        "INFO",
        "Sent OP_GET_OK and %zu file pre-allocation headers for topic '%s'",
        entry->files.size(), entry->topic_name.c_str());
    return 0;
  } else if (hdr.type == OP_JOIN_SESSION) {
    MsgJoinSession req;
    if (!parse_join_session(payload.data(),
                            static_cast<uint32_t>(payload.size()), &req)) {
      log_event("ERROR", "Malformed OP_JOIN_SESSION received on fd=%d",
                client_fd);
      send_error(client_fd, ERR_BAD_REQUEST, "Malformed OP_JOIN_SESSION",
                 wire_counter);
      return -1;
    }

    EnhancedSessionState *sess = find_enhanced_session(req.session_id);
    if (!sess) {
      char hex32[33];
      session_id_to_hex(req.session_id, hex32);
      log_event("ERROR", "Enhanced session '%s' not found for sub-stream %u",
                hex32, req.conn_id);
      send_restart(client_fd, ERR_SESSION_EXPIRED, "Enhanced session not found",
                   wire_counter);
      return -1;
    }

    // Apply client bitmask if provided
    if (!req.chunk_bitmap.empty()) {
      apply_client_bitmap(sess, req.chunk_bitmap.data(),
                          static_cast<uint32_t>(req.chunk_bitmap.size()));
    }

    const TopicEntry *entry = get_topic_path(topic_map, sess->topic);
    if (!entry) {
      send_error(client_fd, ERR_TOPIC_NOT_FOUND, "Topic not found",
                 wire_counter);
      return -1;
    }

    log_event("INFO", "Serving parallel sub-stream %u for topic '%s'",
              req.conn_id, entry->topic_name.c_str());
    return serve_substream_session(client_fd, *entry, sess, req.conn_id, pf,
                                   wire_counter);
  }
  return -1;
}
