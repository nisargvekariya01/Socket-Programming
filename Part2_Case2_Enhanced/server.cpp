#include "common_types.h"
#include "concurrency.h"
#include "directory_mapper.h"
#include "metrics_logger.h"
#if CASE_MODE == 3
#include "enhanced_epoll_server.h"
#endif

#include <iostream>
#include <stdio.h>
#include <unistd.h>

#ifndef PART_NUM
#define PART_NUM 2
#endif

#ifndef CASE_MODE
#define CASE_MODE 3
#endif

int main(int argc, char *argv[]) {
  uint16_t port = 0;    // Holds the port number on which the server listens for
                        // incoming connections
  std::string root_dir; // Holds the file system directory where topics and
                        // payload files are stored
  double pf = 0.0;      // Holds the drop probability
  int threaded_mode =
      1; // Holds the mode of the server (1 for threaded, 0 for sequential)
  uint32_t seed = 0; // Holds the seed for the random number generator

  // Validates port range (1024 <= port <= 65535), directory existence, and
  // (0.0 <= pf <= 1.0)
  if (parse_server_cli(argc, argv, PART_NUM, &port, &root_dir, &pf,
                       &threaded_mode, &seed) != 0) {
    return 1;
  }

  // Registers signal handlers for graceful shutdown on SIGINT, SIGTERM, and
  // SIGPIPE
  register_signal_handlers();

  // Logs the server's starting parameters (Part number, Case mode, Port, drop
  // probability, threaded mode)
  log_event(
      "INFO",
      "Starting Server (Part=%d, CaseMode=%d, Port=%u, pf=%.2f, Threaded=%d)",
      PART_NUM, CASE_MODE, port, pf, threaded_mode);

  std::unordered_map<std::string, TopicEntry> topic_map;
  int topics_count = scan_and_map_topics(root_dir, topic_map);
  if (topics_count < 0) {
    fprintf(stderr, "Failed to load topics from directory: %s\n",
            root_dir.c_str());
    return 1;
  }

  int listen_fd = create_listening_socket(port);
  if (listen_fd < 0) {
    fprintf(stderr, "Failed to initialize server socket on port %u\n", port);
    return 1;
  }

  if (threaded_mode == 0) {
    run_sequential_loop(listen_fd, topic_map, pf, CASE_MODE, seed);
  } else {
#if CASE_MODE == 3
    run_enhanced_epoll_server_loop(listen_fd, topic_map, pf, seed);
#else
    run_concurrent_loop(listen_fd, topic_map, pf, CASE_MODE, seed);
#endif
  }

  log_event("INFO", "Server shutdown complete");
  close(listen_fd);
  return 0;
}
