#ifndef COMMON_TYPES_H
#define COMMON_TYPES_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <time.h>
#include <unordered_map>
#include <vector>

// Protocol Framing Constants
#define PROTOCOL_MAGIC 0x4954           // "IT" in ASCII
#define PROTOCOL_VERSION 0x01           // Protocol Revision 1
#define FRAME_HEADER_SIZE 8             // Fixed 8-byte wire header
#define MAX_FRAME_PAYLOAD (1024 * 1024) // 1 MB sanity ceiling
#define CHUNK_SIZE (64 * 1024)  // 64 KB file transfer & checkpoint window
#define SESSION_TTL_SEC 600     // 600s idle session expiration
#define DEFAULT_MAX_RETRIES 50  // Bounded retry limit for fault recovery
#define DEFAULT_PARALLEL_CONN 4 // N = 4 parallel TCP streams in Enhanced

// Standard Protocol Error & Status Codes
#define ERR_BAD_REQUEST 400
#define ERR_TOPIC_NOT_FOUND 404
#define ERR_TOPIC_MISMATCH 409
#define ERR_FILENAME_MISMATCH 409
#define ERR_SESSION_EXPIRED 410
#define ERR_INVALID_OFFSET 416
#define ERR_HIGHWATER_EXCEED 416
#define ERR_IO_FAILURE 500

// Unified Message Opcodes (enum Opcode : uint8_t)
enum Opcode : uint8_t {
  OP_GET = 0x01,    // C -> S: [topic_len:u16][topic:bytes]
  OP_GET_OK = 0x02, // S -> C: [session_id:16B][file_count:u32][total_bytes:u64]
  OP_ERROR = 0x03,  // S -> C: [err_code:u16][msg_len:u16][reason:bytes]
  OP_FILE_BEGIN =
      0x04, // S -> C:
            // [file_index:u32][byte_offset:u64][bytes_following:u64][name_len:u16][filename:bytes]
  OP_FILE_DATA =
      0x05, // S -> C: [file_index:u32][offset:u64][data_len:u32][data:bytes]
  OP_FILE_END = 0x06,     // S -> C: [file_index:u32]
  OP_TRANSFER_END = 0x07, // S -> C: [files_sent:u32][useful_bytes:u64]
  OP_CHECKPOINT_ACK =
      0x08, // C -> S: [session_id:16B][file_index:u32][ack_offset:u64]
  OP_RESUME =
      0x09, // C -> S:
            // [session_id:16B][file_index:u32][byte_offset:u64][topic_len:u16][topic][name_len:u16][filename]
  OP_RESUME_OK =
      0x0A, // S -> C:
            // [file_index:u32][accepted_offset:u64][rem_files:u32][rem_bytes:u64]
  OP_RESTART = 0x0B, // S -> C: [code:u16][msg_len:u16][reason:bytes]
  OP_GET_ENHANCED =
      0x0C, // C -> S: [num_streams:u16][topic_len:u16][topic:bytes]
  OP_JOIN_SESSION =
      0x0D, // C -> S:
            // [session_id:16B][conn_id:u32][bmap_len:u32][chunk_bitmap:bytes]
  OP_ASSIGN_WORK = 0x0E // S -> C: [conn_id:u32][start_unit:u32][end_unit:u32]
};

// Member 1: Server Directory Mapping & Concurrency Structures
struct FileMeta {
  std::string filename; // Base file name (e.g., "eb10001.jpg")
  std::string
      full_path; // Resolved path (e.g., "/path/to/raw-img/Cane/eb10001.jpg")
  uint64_t file_size; // Exact size in bytes from stat()
};

struct TopicEntry {
  std::string topic_name;      // Folder name key (e.g., "Cane")
  std::string dir_path;        // Canonical path to topic directory
  std::vector<FileMeta> files; // Deterministically sorted by filename
  uint64_t total_bytes;        // Sum of all file_size values in this topic
};

// Worker Thread Arguments
struct WorkerArgs {
  int client_fd;
  const std::unordered_map<std::string, TopicEntry> *topic_map;
  double pf;
  int case_mode; // 0 = Part1, 1 = Case1, 2 = Case2, 3 = Enhanced
  uint32_t rng_seed;
};

// Wire Frame Header
struct FrameHeader {
  uint16_t magic;          // 0x4954 ("IT")
  uint8_t version;         // 0x01
  uint8_t type;            // Opcode (0x01 - 0x0E)
  uint32_t payload_length; // Length of payload in bytes (0 .. 1048576)
};

// Client Checkpoint Record
struct CheckpointRecord {
  uint8_t session_id[16];  // 128-bit binary ID for wire messages
  char session_id_hex[33]; // 32-char lowercase hex + '\0' for disk .ckpt
  char topic[64];          // Topic name string
  uint32_t file_index;     // 0 .. file_count (equals file_count at EOF)
  uint64_t byte_offset;    // Durably fsync'd byte offset in current file
  uint32_t files_done;     // Count of completely transferred files
  char
      file_name[256]; // Current filename or "EOF" when file_index == file_count
};

// Server Session State
struct ServerSession {
  uint8_t session_id[16];        // 128-bit binary session ID
  char session_id_hex[33];       // Map key: 32-char hex string
  char topic[64];                // Assigned topic
  uint32_t file_index;           // Highwater file index reached by server
  uint64_t bytes_sent_highwater; // Cumulative payload bytes sent by server
                                 // across topic
  uint64_t current_file_sent;    // Bytes sent in the current file_index
  time_t last_active;  // Updated via time(NULL) on activity (600s TTL)
  bool all_files_sent; // True once server finishes sending final file
};

// Case 2 Enhanced Chunk Lifecycle & Work Unit
enum ChunkLifecycle : uint8_t {
  CHUNK_UNASSIGNED = 0,
  CHUNK_IN_FLIGHT = 1,
  CHUNK_COMMITTED = 2
};

struct ChunkWorkUnit {
  uint32_t
      unit_id; // Global 64 KiB chunk index across topic (0 .. total_units-1)
  uint32_t file_index;      // Index into TopicEntry.files
  uint64_t file_offset;     // Byte offset within that file (multiple of 64 KiB)
  uint32_t chunk_len;       // Bytes in this unit (<= 65536)
  ChunkLifecycle state;     // UNASSIGNED -> IN_FLIGHT -> COMMITTED
  int32_t assigned_conn_id; // Sub-stream ID currently serving this unit (-1 if
                            // none)
};

// Enhanced Session State
struct EnhancedSessionState {
  uint8_t session_id[16];
  std::string topic;
  uint16_t num_streams;
  std::vector<ChunkWorkUnit> units;
  pthread_mutex_t queue_mutex;
  time_t last_active;
};

// Client Shared Bitmask State
struct SharedBitmaskState {
  std::string topic;
  std::string output_dir;
  const TopicEntry *topic_meta;
  std::vector<int> open_fds; // Pre-allocated file descriptors for pwrite()
  std::vector<uint8_t>
      chunk_bitmap; // 1 bit per 64 KiB unit (1 = written on disk)
  uint32_t total_units;
  uint32_t committed_units;
  uint64_t useful_bytes;
  uint64_t total_wire_bytes;
  uint64_t redundant_bytes;
  int retries;
  pthread_mutex_t bmap_mutex;
};

#endif // COMMON_TYPES_H
