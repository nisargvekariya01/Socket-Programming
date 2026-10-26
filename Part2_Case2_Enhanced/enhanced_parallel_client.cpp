#include "enhanced_parallel_client.h"
#include "fault.h"
#include "metrics_logger.h"
#include "protocol.h"

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

static std::atomic<int> g_active_streams{0};
static int g_configured_streams = 0;
static uint64_t g_expected_total_bytes = 0;
static struct timespec g_transfer_start{0, 0};

/*
 * enhanced_parallel_client.cpp - Member 5 (PR #6, Wave 3)
 *
 * Implements:
 *   1. Parallel TCP sub-stream pool (N = 4 streams default, configurable via
 * CLI)
 *   2. Target file pre-allocation via posix_fallocate() / ftruncate()
 *   3. Lock-free concurrent client file assembly via positional pwrite()
 *   4. Sub-stream fault isolation and dynamic bitmask resynchronization
 *   5. Entry point run_enhanced_client() invoked by client.cpp
 */

// Makes sure all parent folders exist on your computer before creating a
// file inside them so the program never crashes with "Folder not found".
static void ensure_parent_dirs(const std::string &filepath) {
  size_t last_slash = filepath.find_last_of("/\\");
  if (last_slash != std::string::npos) {
    std::string dir = filepath.substr(0, last_slash);
    std::string current;
    for (size_t i = 0; i < dir.size(); ++i) {
      char c = dir[i];
      if (c == '/' || c == '\\') {
        if (!current.empty() && current != "." && current != "..") {
          mkdir(current.c_str(), 0755);
        }
      }
      current += c;
    }
    if (!current.empty()) {
      mkdir(current.c_str(), 0755);
    }
  }
}

// Creates an empty file on your computer that is exactly the final size
// you expect. This prevents the file from growing slowly (which causes
// fragmentation) and ensures the OS reserves the space immediately.
int preallocate_target_file(const std::string &path, uint64_t file_size) {
  ensure_parent_dirs(path);
  int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    log_event("ERROR", "open failed for '%s': %s", path.c_str(),
              strerror(errno));
    return -1;
  }

  if (file_size > 0) {
    int prc = posix_fallocate(fd, 0, static_cast<off_t>(file_size));
    if (prc != 0) {
      if (ftruncate(fd, static_cast<off_t>(file_size)) != 0) {
        log_event("WARNING", "ftruncate fallback failed for '%s': %s",
                  path.c_str(), strerror(errno));
      }
    }
  } else {
    if (ftruncate(fd, 0) != 0) {
      log_event("WARNING", "ftruncate(0) failed for '%s': %s", path.c_str(),
                strerror(errno));
    }
  }

  return fd;
}

// Draw a cool progress bar that shows how fast the file is being downloaded
// and how much is left.
static void render_enhanced_progress_bar(SharedBitmaskState *state,
                                         int active_streams, int total_streams,
                                         uint64_t total_bytes,
                                         const struct timespec &start_time,
                                         bool force_complete = false) {
  static const char spinner_chars[] = "|/-\\";
  static int spinner_idx = 0;
  static struct timespec last_update{0, 0};
  // If running in benchmark mode with concurrent clients, avoid stdout TTY
  // mutex contention
  const char *env_nc = getenv("NUM_CLIENTS");
  if (env_nc && atoi(env_nc) > 1 && !force_complete) {
    return;
  }

  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);

  // Throttle updates to ~10 FPS (100ms) unless force_complete
  if (!force_complete && last_update.tv_sec != 0) {
    long elapsed_ms = (now.tv_sec - last_update.tv_sec) * 1000 +
                      (now.tv_nsec - last_update.tv_nsec) / 1000000;
    if (elapsed_ms < 100) {
      return;
    }
  }
  last_update = now;

  uint32_t committed_units = state ? state->committed_units : 0;
  uint32_t total_units = state ? state->total_units : 0;
  uint64_t useful_bytes = state ? state->useful_bytes : 0;
  uint64_t redundant_bytes = state ? state->redundant_bytes : 0;

  static bool is_terminal = (isatty(STDOUT_FILENO) != 0);
  if (!is_terminal) {
    if (force_complete ||
        (total_units > 0 && committed_units > 0 &&
         (committed_units % 100 == 0 || committed_units == total_units))) {
      double pct = total_bytes > 0
                       ? (static_cast<double>(useful_bytes) * 100.0) /
                             static_cast<double>(total_bytes)
                       : (total_units > 0
                              ? (static_cast<double>(committed_units) * 100.0) /
                                    static_cast<double>(total_units)
                              : 0.0);
      double mb = static_cast<double>(useful_bytes) / (1024.0 * 1024.0);
      log_event(
          "INFO",
          "[Parallel %d/%d Streams] Progress: %u/%u units (%.1f%%) - %.2f MB",
          active_streams, total_streams, committed_units, total_units, pct, mb);
    }
    return;
  }

  // Cache terminal width to avoid frequent ioctl syscalls
  static int cached_cols = 80;
  static time_t last_cols_query = 0;
  if (now.tv_sec - last_cols_query >= 2) {
    struct winsize ws;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_col > 20) {
      cached_cols = ws.ws_col;
    }
    last_cols_query = now.tv_sec;
  }
  int cols = cached_cols;

  double pct = 0.0;
  if (total_bytes > 0) {
    pct =
        (static_cast<double>(useful_bytes) / static_cast<double>(total_bytes)) *
        100.0;
  } else if (total_units > 0) {
    pct = (static_cast<double>(committed_units) /
           static_cast<double>(total_units)) *
          100.0;
  }
  if (pct > 100.0)
    pct = 100.0;
  if (force_complete)
    pct = 100.0;

  double elapsed_sec = (double)(now.tv_sec - start_time.tv_sec) +
                       (double)(now.tv_nsec - start_time.tv_nsec) * 1e-9;
  double speed_mbps = (elapsed_sec > 0.05)
                          ? (static_cast<double>(useful_bytes) /
                             (elapsed_sec * 1024.0 * 1024.0))
                          : 0.0;

  char spinner = spinner_chars[spinner_idx % 4];
  spinner_idx++;

  double cur_mb = static_cast<double>(useful_bytes) / (1024.0 * 1024.0);
  double tot_mb = static_cast<double>(total_bytes) / (1024.0 * 1024.0);
  double waste_mb = static_cast<double>(redundant_bytes) / (1024.0 * 1024.0);

  char streams_buf[40];
  if (cols >= 86) {
    snprintf(streams_buf, sizeof(streams_buf), "[Streams: %d/%d]",
             active_streams, total_streams);
  } else {
    snprintf(streams_buf, sizeof(streams_buf), "[%d/%d-STR]", active_streams,
             total_streams);
  }
  int streams_len = static_cast<int>(strlen(streams_buf));

  char units_buf[32];
  if (cols >= 95) {
    snprintf(units_buf, sizeof(units_buf), "%u/%u Units", committed_units,
             total_units);
  } else {
    snprintf(units_buf, sizeof(units_buf), "%u/%uU", committed_units,
             total_units);
  }
  int units_len = static_cast<int>(strlen(units_buf));

  char mb_buf[40];
  if (cols >= 95) {
    snprintf(mb_buf, sizeof(mb_buf), "%.1f/%.1f MB", cur_mb, tot_mb);
  } else {
    snprintf(mb_buf, sizeof(mb_buf), "%.1f/%.1fM", cur_mb, tot_mb);
  }
  int mb_len = static_cast<int>(strlen(mb_buf));

  char speed_buf[32];
  if (cols >= 90) {
    snprintf(speed_buf, sizeof(speed_buf), "%.2f MB/s", speed_mbps);
  } else {
    snprintf(speed_buf, sizeof(speed_buf), "%.1fMB/s", speed_mbps);
  }
  int speed_len = static_cast<int>(strlen(speed_buf));

  char waste_display[48] = "";
  int waste_vis_len = 0;
  if (redundant_bytes > 0) {
    if (cols >= 100) {
      snprintf(waste_display, sizeof(waste_display),
               " | \033[1;31mWasted: %.1fMB\033[0m", waste_mb);
      char tmp[32];
      snprintf(tmp, sizeof(tmp), " | Wasted: %.1fMB", waste_mb);
      waste_vis_len = static_cast<int>(strlen(tmp));
    } else if (cols >= 86) {
      snprintf(waste_display, sizeof(waste_display),
               " | \033[1;31mW:%.1fM\033[0m", waste_mb);
      char tmp[32];
      snprintf(tmp, sizeof(tmp), " | W:%.1fM", waste_mb);
      waste_vis_len = static_cast<int>(strlen(tmp));
    }
  }

  int tail_len = force_complete ? 13 : 4;
  int non_bar_len = streams_len + 1 + 2 + 1 + 7 + 3 + units_len + 3 + mb_len +
                    waste_vis_len + 3 + speed_len + tail_len;

  int max_vis = cols - 2;
  int bar_width = max_vis - non_bar_len;
  if (bar_width < 5)
    bar_width = 5;
  if (bar_width > 22)
    bar_width = 22;

  int filled = static_cast<int>((pct / 100.0) * bar_width);
  if (filled > bar_width)
    filled = bar_width;

  char bar_str[bar_width + 1];
  for (int i = 0; i < bar_width; ++i) {
    if (i < filled) {
      bar_str[i] = '=';
    } else if (i == filled && filled < bar_width) {
      bar_str[i] = '>';
    } else {
      bar_str[i] = ' ';
    }
  }
  bar_str[bar_width] = '\0';

  if (force_complete) {
    printf("\r\033[2K\033[1;35m%s\033[0m \033[1;32m[%s]\033[0m "
           "\033[1;32m100.0%%\033[0m | \033[1;36m%s\033[0m | "
           "\033[1;36m%s\033[0m%s | \033[1;33m%s\033[0m | \033[1;32mDone! "
           "[OK]\033[0m\n",
           streams_buf, bar_str, units_buf, mb_buf, waste_display, speed_buf);
  } else {
    printf("\r\033[2K\033[1;35m%s\033[0m \033[1;32m[%s]\033[0m "
           "\033[1;32m%5.1f%%\033[0m | \033[1;36m%s\033[0m | "
           "\033[1;36m%s\033[0m%s | \033[1;33m%s\033[0m [%c]",
           streams_buf, bar_str, pct, units_buf, mb_buf, waste_display,
           speed_buf, spinner);
  }
  fflush(stdout);
}

// "Marks" a chunk as received. It checks if this chunk has already been
// marked by another stream. If yes, it counts it as "wasted data" (redundant).
// If no, it marks it as received and adds the bytes to the total.
bool mark_chunk_committed(SharedBitmaskState *state, uint32_t unit_id,
                          uint32_t chunk_len) {
  if (!state)
    return false;
  pthread_mutex_lock(&state->bmap_mutex);
  bool is_new = false;
  if (unit_id < state->total_units) {
    size_t byte_idx = unit_id / 8;
    uint8_t bit_mask = static_cast<uint8_t>(1 << (unit_id % 8));
    if ((state->chunk_bitmap[byte_idx] & bit_mask) == 0) {
      state->chunk_bitmap[byte_idx] |= bit_mask;
      state->committed_units++;
      state->useful_bytes += chunk_len;
      is_new = true;
    } else {
      state->redundant_bytes += chunk_len;
    }
  }
  uint32_t committed = state->committed_units;
  pthread_mutex_unlock(&state->bmap_mutex);
  if (committed % 16 == 0) {
    render_enhanced_progress_bar(state, g_active_streams.load(),
                                 g_configured_streams, g_expected_total_bytes,
                                 g_transfer_start);
  }
  return is_new;
}

// Checks if every single 64KB chunk has been received and marked.
bool is_transfer_complete(SharedBitmaskState *state) {
  if (!state)
    return false;
  pthread_mutex_lock(&state->bmap_mutex);
  bool complete = (state->committed_units >= state->total_units);
  pthread_mutex_unlock(&state->bmap_mutex);
  return complete;
}

// This is the brain of each individual thread. It keeps trying to grab work,
// receive data, and write it to the file. If it fails, it waits and tries
// again, but never more than allowed.
void *client_parallel_worker(void *arg) {
  EnhancedClientWorkerArgs *args = static_cast<EnhancedClientWorkerArgs *>(arg);
  SharedBitmaskState *state = args->shared_state;
  uint32_t conn_id = args->conn_id;

  for (int attempt = 0; attempt < args->max_retries; ++attempt) {
    if (is_transfer_complete(state)) {
      break;
    }

    if (attempt > 0) {
      pthread_mutex_lock(&state->bmap_mutex);
      state->retries++;
      pthread_mutex_unlock(&state->bmap_mutex);

      int shift = (attempt < 10) ? attempt : 10;
      int delay_ms = std::min(1000, 25 * (1 << shift));
      log_event("INFO", "Sub-stream %u retrying attempt %d after %d ms...",
                conn_id, attempt, delay_ms);
      struct timespec ts;
      ts.tv_sec = delay_ms / 1000;
      ts.tv_nsec = (delay_ms % 1000) * 1000000L;
      nanosleep(&ts, nullptr);
    }

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
      continue;
    }

    struct sockaddr_in saddr;
    std::memset(&saddr, 0, sizeof(saddr));
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(args->port);
    inet_pton(AF_INET, args->server_ip.c_str(), &saddr.sin_addr);

    if (connect(sockfd, reinterpret_cast<struct sockaddr *>(&saddr),
                sizeof(saddr)) < 0) {
      close(sockfd);
      continue;
    }

    // Disable Nagle's algorithm + tune socket buffers to 256 KiB
    {
      int nd = 1, bsz = 256 * 1024;
      setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));
      setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &bsz, sizeof(bsz));
      setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &bsz, sizeof(bsz));
    }

    // Take thread-safe snapshot of bitmask for resynchronization
    std::vector<uint8_t> bmap_snapshot;
    pthread_mutex_lock(&state->bmap_mutex);
    bmap_snapshot = state->chunk_bitmap;
    pthread_mutex_unlock(&state->bmap_mutex);

    uint64_t stream_wire = 0;
    int jrc = send_join_session(
        sockfd, args->session_id, conn_id, bmap_snapshot.data(),
        static_cast<uint32_t>(bmap_snapshot.size()), &stream_wire);
    if (jrc < 0) {
      close(sockfd);
      continue;
    }

    g_active_streams++;
    if (attempt > 0 && isatty(STDOUT_FILENO)) {
      pthread_mutex_lock(&state->bmap_mutex);
      uint32_t c_units = state->committed_units;
      uint32_t t_units = state->total_units;
      pthread_mutex_unlock(&state->bmap_mutex);
      printf("\n\033[1;36m[SUB-STREAM RECONNECTED]\033[0m Sub-stream %u "
             "rejoined session (confirmed %u/%u units on disk)\n",
             conn_id, c_units, t_units);
      fflush(stdout);
    }

    bool stream_faulted = false;
    while (!is_transfer_complete(state)) {
      FrameHeader hdr;
      std::vector<uint8_t> payload;
      int rrc = recv_frame(sockfd, &hdr, payload, &stream_wire);
      if (rrc <= 0) {
        // Connection dropped due to fault injection or network event
        stream_faulted = true;
        g_active_streams--;
        int surviving = g_active_streams.load();
        if (surviving < 0)
          surviving = 0;
        if (isatty(STDOUT_FILENO)) {
          const char *fault_desc = (rrc == 0)
                                       ? "Connection closed by server"
                                       : "Connection dropped / fault injected";
          printf("\n\033[1;33m[FAULT DETECTED / SUB-STREAM DROP]\033[0m "
                 "Sub-stream %u disconnected (%s). %d surviving stream(s) "
                 "continue. Reconnecting with bitmask (Retry %d/%d)...\n",
                 conn_id, fault_desc, surviving, attempt + 2,
                 args->max_retries);
          fflush(stdout);
        }
        log_event("INFO",
                  "Sub-stream %u disconnected (rc=%d). Surviving streams "
                  "continue uninterrupted.",
                  conn_id, rrc);
        break;
      }

      if (hdr.type == OP_FILE_DATA) {
        MsgFileData fd_msg;
        if (!parse_file_data(payload.data(),
                             static_cast<uint32_t>(payload.size()), &fd_msg)) {
          log_event("ERROR", "Sub-stream %u received malformed OP_FILE_DATA",
                    conn_id);
          stream_faulted = true;
          break;
        }

        if (fd_msg.file_index < state->open_fds.size() &&
            state->open_fds[fd_msg.file_index] >= 0) {
          int target_fd = state->open_fds[fd_msg.file_index];
          size_t to_write = fd_msg.data.size();
          size_t written = 0;

          // LOCK-FREE POSITIONAL PWRITE DIRECTLY INTO PRE-ALLOCATED FILE
          while (written < to_write) {
            ssize_t w = pwrite(target_fd, fd_msg.data.data() + written,
                               to_write - written,
                               static_cast<off_t>(fd_msg.offset + written));
            if (w <= 0) {
              log_event("ERROR", "pwrite failed on fd=%d offset=%llu: %s",
                        target_fd,
                        static_cast<unsigned long long>(fd_msg.offset),
                        strerror(errno));
              break;
            }
            written += static_cast<size_t>(w);
          }

          // Update bitmask and accounting
          if (fd_msg.file_index < args->file_chunk_bases.size()) {
            uint32_t unit_id = static_cast<uint32_t>(
                args->file_chunk_bases[fd_msg.file_index] +
                (fd_msg.offset / CHUNK_SIZE));
            mark_chunk_committed(state, unit_id,
                                 static_cast<uint32_t>(fd_msg.data.size()));
          }
        }
      } else if (hdr.type == OP_TRANSFER_END) {
        break;
      } else if (hdr.type == OP_ERROR || hdr.type == OP_RESTART) {
        if (isatty(STDOUT_FILENO))
          printf("\n");
        log_event("INFO", "Sub-stream %u received opcode %u", conn_id,
                  hdr.type);
        break;
      }
    }

    // Add local stream wire counter to total
    pthread_mutex_lock(&state->bmap_mutex);
    state->total_wire_bytes += stream_wire;
    pthread_mutex_unlock(&state->bmap_mutex);

    close(sockfd);

    if (!stream_faulted) {
      g_active_streams--;
    }

    if (!stream_faulted && is_transfer_complete(state)) {
      break;
    }
  }

  delete args;
  return nullptr;
}

// Main setup function. It connects, creates the shared bitmask, spawns the
// worker threads, and waits for the download to finish.
int run_enhanced_client(const char *server_ip, uint16_t port, const char *topic,
                        const char *output_dir, int num_streams,
                        int max_retries) {
  if (!server_ip || !topic || !output_dir) {
    return -1;
  }

  if (num_streams <= 0) {
    num_streams = DEFAULT_PARALLEL_CONN;
  }
  if (max_retries <= 0) {
    max_retries = DEFAULT_MAX_RETRIES;
  }

  struct timespec t_start, t_end;
  clock_gettime(CLOCK_MONOTONIC, &t_start);

  uint64_t total_wire_bytes = 0;

  // Phase 1: Connect via control socket to initiate session
  int control_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (control_fd < 0) {
    log_event("ERROR", "Failed to create control socket: %s", strerror(errno));
    return -1;
  }

  struct sockaddr_in saddr;
  std::memset(&saddr, 0, sizeof(saddr));
  saddr.sin_family = AF_INET;
  saddr.sin_port = htons(port);
  inet_pton(AF_INET, server_ip, &saddr.sin_addr);

  if (connect(control_fd, reinterpret_cast<struct sockaddr *>(&saddr),
              sizeof(saddr)) < 0) {
    log_event("ERROR", "Control socket failed to connect to %s:%u: %s",
              server_ip, port, strerror(errno));
    close(control_fd);
    return -1;
  }

  // Disable Nagle's algorithm + tune socket buffers to 256 KiB
  {
    int nd = 1, bsz = 256 * 1024;
    setsockopt(control_fd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));
    setsockopt(control_fd, SOL_SOCKET, SO_SNDBUF, &bsz, sizeof(bsz));
    setsockopt(control_fd, SOL_SOCKET, SO_RCVBUF, &bsz, sizeof(bsz));
  }

  if (send_get_enhanced(control_fd, static_cast<uint16_t>(num_streams), topic,
                        &total_wire_bytes) < 0) {
    log_event("ERROR", "Failed to send OP_GET_ENHANCED for topic '%s'", topic);
    close(control_fd);
    return -1;
  }

  // Receive OP_GET_OK
  FrameHeader hdr;
  std::vector<uint8_t> payload;
  int rrc = recv_frame(control_fd, &hdr, payload, &total_wire_bytes);
  if (rrc <= 0 || hdr.type != OP_GET_OK) {
    log_event("ERROR",
              "Failed to receive OP_GET_OK from server (rrc=%d, type=%u)", rrc,
              hdr.type);
    close(control_fd);
    return -1;
  }

  MsgGetOk get_ok;
  if (!parse_get_ok(payload.data(), static_cast<uint32_t>(payload.size()),
                    &get_ok)) {
    log_event("ERROR", "Malformed OP_GET_OK received");
    close(control_fd);
    return -1;
  }

  uint8_t session_id[16];
  std::memcpy(session_id, get_ok.session_id, 16);
  char hex32[33];
  session_id_to_hex(session_id, hex32);
  log_event("INFO", "Session established: id='%s', files=%u, total_bytes=%llu",
            hex32, get_ok.file_count,
            static_cast<unsigned long long>(get_ok.total_bytes));

  // Receive OP_FILE_BEGIN for each file in the topic
  std::vector<FileMeta> files(get_ok.file_count);
  for (uint32_t i = 0; i < get_ok.file_count; ++i) {
    rrc = recv_frame(control_fd, &hdr, payload, &total_wire_bytes);
    if (rrc <= 0 || hdr.type != OP_FILE_BEGIN) {
      log_event("ERROR",
                "Expected OP_FILE_BEGIN for file %u, got rc=%d type=%u", i, rrc,
                hdr.type);
      close(control_fd);
      return -1;
    }

    MsgFileBegin fb;
    if (!parse_file_begin(payload.data(), static_cast<uint32_t>(payload.size()),
                          &fb)) {
      log_event("ERROR", "Malformed OP_FILE_BEGIN received for file %u", i);
      close(control_fd);
      return -1;
    }

    if (fb.file_index < files.size()) {
      files[fb.file_index].filename = fb.filename;
      files[fb.file_index].file_size = fb.bytes_following;
      files[fb.file_index].full_path = fb.filename;
    }
  }

  close(control_fd);

  // Phase 2: Pre-allocate target files on disk
  std::string topic_dir = std::string(output_dir) + "/" + topic;
  std::vector<int> open_fds(files.size(), -1);
  for (size_t f = 0; f < files.size(); ++f) {
    std::string full_dest = topic_dir + "/" + files[f].filename;
    int fd = preallocate_target_file(full_dest, files[f].file_size);
    if (fd < 0) {
      log_event("ERROR", "Failed to pre-allocate file '%s'", full_dest.c_str());
      for (int ofd : open_fds) {
        if (ofd >= 0)
          close(ofd);
      }
      return -1;
    }
    open_fds[f] = fd;
  }

  // Phase 3: Setup work units mapping and bitmask
  uint32_t total_units = 0;
  std::vector<uint64_t> file_chunk_bases(files.size(), 0);
  for (size_t f = 0; f < files.size(); ++f) {
    file_chunk_bases[f] = total_units;
    uint64_t fsize = files[f].file_size;
    if (fsize == 0) {
      total_units++;
    } else {
      total_units +=
          static_cast<uint32_t>((fsize + CHUNK_SIZE - 1) / CHUNK_SIZE);
    }
  }

  SharedBitmaskState shared_state;
  shared_state.topic = topic;
  shared_state.output_dir = output_dir;
  shared_state.topic_meta = nullptr;
  shared_state.open_fds = open_fds;
  shared_state.total_units = total_units;
  shared_state.committed_units = 0;
  shared_state.chunk_bitmap.assign((total_units + 7) / 8, 0);
  shared_state.useful_bytes = 0;
  shared_state.total_wire_bytes = total_wire_bytes;
  shared_state.redundant_bytes = 0;
  shared_state.retries = 0;
  pthread_mutex_init(&shared_state.bmap_mutex, nullptr);

  // Mark 0-byte file units as committed immediately
  for (size_t f = 0; f < files.size(); ++f) {
    if (files[f].file_size == 0) {
      uint32_t u_id = static_cast<uint32_t>(file_chunk_bases[f]);
      shared_state.chunk_bitmap[u_id / 8] |=
          static_cast<uint8_t>(1 << (u_id % 8));
      shared_state.committed_units++;
    }
  }

  g_active_streams.store(0);
  g_configured_streams = num_streams;
  g_expected_total_bytes = get_ok.total_bytes;
  g_transfer_start = t_start;

  double total_mb = static_cast<double>(get_ok.total_bytes) / (1024.0 * 1024.0);
  log_event("INFO",
            "Initialized parallel client: %u streams, %u 64 KiB chunks across "
            "%zu files (%.2f MB)",
            num_streams, total_units, files.size(), total_mb);

  pthread_mutex_lock(&shared_state.bmap_mutex);
  render_enhanced_progress_bar(&shared_state, 0, num_streams,
                               get_ok.total_bytes, t_start);
  pthread_mutex_unlock(&shared_state.bmap_mutex);

  // Phase 4: Launch parallel sub-stream worker pool
  std::vector<pthread_t> threads(num_streams);
  for (int i = 0; i < num_streams; ++i) {
    EnhancedClientWorkerArgs *args = new EnhancedClientWorkerArgs();
    args->server_ip = server_ip;
    args->port = port;
    args->conn_id = static_cast<uint32_t>(i);
    std::memcpy(args->session_id, session_id, 16);
    args->max_retries = max_retries;
    args->shared_state = &shared_state;
    args->file_chunk_bases = file_chunk_bases;
    for (const auto &fm : files) {
      args->file_sizes.push_back(fm.file_size);
    }
    pthread_create(&threads[i], nullptr, client_parallel_worker, args);
  }

  // Wait for all sub-streams to complete
  for (int i = 0; i < num_streams; ++i) {
    pthread_join(threads[i], nullptr);
  }

  pthread_mutex_lock(&shared_state.bmap_mutex);
  render_enhanced_progress_bar(&shared_state, num_streams, num_streams,
                               shared_state.useful_bytes, t_start,
                               true /* force_complete */);
  pthread_mutex_unlock(&shared_state.bmap_mutex);

  // Phase 5: Flush and close all pre-allocated files
  for (int ofd : open_fds) {
    if (ofd >= 0) {
      fsync(ofd);
      close(ofd);
    }
  }

  clock_gettime(CLOCK_MONOTONIC, &t_end);

  bool success = (shared_state.committed_units >= shared_state.total_units);
  if (!success) {
    log_event("ERROR",
              "Enhanced parallel transfer failed: committed %u/%u units after "
              "%d retries",
              shared_state.committed_units, shared_state.total_units,
              shared_state.retries);
    pthread_mutex_destroy(&shared_state.bmap_mutex);
    return -1;
  }

  std::string csv_path = "Results/Part2_metrics.csv";
  if (access("Results", F_OK) != 0 && access("../Results", F_OK) == 0) {
    csv_path = "../Results/Part2_metrics.csv";
  }

  record_transfer_metrics(
      csv_path.c_str(), "Part2_Case2_Enhanced", num_streams, 0.0,
      shared_state.useful_bytes, shared_state.total_wire_bytes,
      shared_state.redundant_bytes, t_start, t_end, shared_state.retries);

  double duration_sec = (double)(t_end.tv_sec - t_start.tv_sec) +
                        (double)(t_end.tv_nsec - t_start.tv_nsec) * 1e-9;
  if (duration_sec < 0.0)
    duration_sec = 0.0;
  double goodput_mbps =
      (duration_sec > 0.0)
          ? (shared_state.useful_bytes / (duration_sec * 1000000.0))
          : 0.0;
  double throughput_mbps =
      (duration_sec > 0.0)
          ? (shared_state.total_wire_bytes / (duration_sec * 1000000.0))
          : 0.0;
  double overhead_pct = (shared_state.useful_bytes > 0)
                            ? (((double)shared_state.total_wire_bytes -
                                (double)shared_state.useful_bytes) /
                               (double)shared_state.useful_bytes) *
                                  100.0
                            : 0.0;

  log_event("INFO",
            "Enhanced parallel transfer SUCCESS: %zu files (%llu bytes) "
            "written to '%s' in %d retries",
            files.size(),
            static_cast<unsigned long long>(shared_state.useful_bytes),
            topic_dir.c_str(), shared_state.retries);
  log_event("INFO",
            "Performance: Duration = %.2fs | Goodput = %.2f MB/s | Wire "
            "Throughput = %.2f MB/s | Overhead = %.2f%%",
            duration_sec, goodput_mbps, throughput_mbps, overhead_pct);

  pthread_mutex_destroy(&shared_state.bmap_mutex);
  return 0;
}
