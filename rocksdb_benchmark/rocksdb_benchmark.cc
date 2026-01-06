// RocksDB Local Benchmark with Tunable Duration and Intensity
//
// Usage:
//   bazel run //rocksdb_benchmark:rocksdb_benchmark -- [flags]
//
// Example flags:
//   --duration_secs=60      Duration of benchmark in seconds
//   --num_threads=4         Number of worker threads
//   --ops_per_thread=100000 Operations per thread (when duration_secs=0)
//   --key_size=16           Size of keys in bytes
//   --value_size=100        Size of values in bytes
//   --read_percent=80       Percentage of read operations

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"

// Duration flags
ABSL_FLAG(int32_t, duration_secs, 30,
          "Duration of the benchmark in seconds. Set to 0 for ops-based mode.");

// Intensity flags
ABSL_FLAG(int32_t, num_threads, 5,
          "Number of worker threads for the benchmark.");
ABSL_FLAG(int64_t, ops_per_thread, 100000,
          "Number of operations per thread (used when duration_secs=0).");
ABSL_FLAG(int32_t, batch_size, 1,
          "Number of operations per batch write (1 = no batching).");

// Workload flags
ABSL_FLAG(int32_t, read_percent, 50,
          "Percentage of read operations (0-100). Rest are writes.");
ABSL_FLAG(int32_t, key_size, 16, "Size of keys in bytes.");
ABSL_FLAG(int32_t, value_size, 512, "Size of values in bytes.");
ABSL_FLAG(int64_t, num_keys, 50000,
          "Total number of unique keys in the key space.");

// DB configuration flags
ABSL_FLAG(std::string, db_path, "/tmp/rocksdb_benchmark",
          "Path to the RocksDB database directory.");
ABSL_FLAG(bool, use_existing_db, false,
          "If true, use existing database. If false, create fresh DB.");
ABSL_FLAG(int32_t, write_buffer_size_mb, 64,
          "Write buffer size in MB.");
ABSL_FLAG(int32_t, max_write_buffer_number, 3,
          "Maximum number of write buffers.");
ABSL_FLAG(bool, enable_compression, true,
          "Enable compression (snappy) for SST files.");
ABSL_FLAG(bool, sync_writes, false,
          "Sync writes to disk (slower but safer).");

namespace {

// Statistics tracking
struct BenchmarkStats {
  std::atomic<int64_t> reads_completed{0};
  std::atomic<int64_t> writes_completed{0};
  std::atomic<int64_t> read_latency_ns{0};
  std::atomic<int64_t> write_latency_ns{0};
  std::atomic<int64_t> errors{0};
};

// Generate a random key based on key_id
std::string GenerateKey(int64_t key_id, int key_size) {
  std::string key = absl::StrFormat("%0*d", key_size, key_id % absl::GetFlag(FLAGS_num_keys));
  if (key.size() > static_cast<size_t>(key_size)) {
    key = key.substr(0, key_size);
  }
  return key;
}

// Generate a random value
std::string GenerateValue(absl::BitGen& gen, int value_size) {
  std::string value;
  value.reserve(value_size);
  for (int i = 0; i < value_size; ++i) {
    value.push_back('a' + absl::Uniform(gen, 0, 26));
  }
  return value;
}

// Worker thread function for time-based benchmark
void WorkerThreadTimeBased(rocksdb::DB* db, int thread_id,
                           absl::Duration duration,
                           BenchmarkStats* stats,
                           std::atomic<bool>* stop_flag) {
  absl::BitGen gen;
  const int key_size = absl::GetFlag(FLAGS_key_size);
  const int value_size = absl::GetFlag(FLAGS_value_size);
  const int read_percent = absl::GetFlag(FLAGS_read_percent);
  const int64_t num_keys = absl::GetFlag(FLAGS_num_keys);
  const int batch_size = absl::GetFlag(FLAGS_batch_size);
  const bool sync_writes = absl::GetFlag(FLAGS_sync_writes);

  rocksdb::ReadOptions read_opts;
  rocksdb::WriteOptions write_opts;
  write_opts.sync = sync_writes;

  while (!stop_flag->load(std::memory_order_relaxed)) {
    int64_t key_id = absl::Uniform(gen, 0, num_keys);
    std::string key = GenerateKey(key_id, key_size);

    if (absl::Uniform(gen, 0, 100) < read_percent) {
      // Read operation
      auto start = std::chrono::high_resolution_clock::now();
      std::string value;
      rocksdb::Status s = db->Get(read_opts, key, &value);
      auto end = std::chrono::high_resolution_clock::now();

      if (s.ok() || s.IsNotFound()) {
        stats->reads_completed.fetch_add(1, std::memory_order_relaxed);
        stats->read_latency_ns.fetch_add(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count(),
            std::memory_order_relaxed);
      } else {
        stats->errors.fetch_add(1, std::memory_order_relaxed);
      }
    } else {
      // Write operation
      auto start = std::chrono::high_resolution_clock::now();
      rocksdb::Status s;

      if (batch_size > 1) {
        rocksdb::WriteBatch batch;
        for (int i = 0; i < batch_size; ++i) {
          int64_t batch_key_id = absl::Uniform(gen, 0, num_keys);
          std::string batch_key = GenerateKey(batch_key_id, key_size);
          std::string value = GenerateValue(gen, value_size);
          batch.Put(batch_key, value);
        }
        s = db->Write(write_opts, &batch);
      } else {
        std::string value = GenerateValue(gen, value_size);
        s = db->Put(write_opts, key, value);
      }
      auto end = std::chrono::high_resolution_clock::now();

      if (s.ok()) {
        stats->writes_completed.fetch_add(batch_size, std::memory_order_relaxed);
        stats->write_latency_ns.fetch_add(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count(),
            std::memory_order_relaxed);
      } else {
        stats->errors.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }
}

// Worker thread function for ops-based benchmark
void WorkerThreadOpsBased(rocksdb::DB* db, int thread_id,
                          int64_t ops_count, BenchmarkStats* stats) {
  absl::BitGen gen;
  const int key_size = absl::GetFlag(FLAGS_key_size);
  const int value_size = absl::GetFlag(FLAGS_value_size);
  const int read_percent = absl::GetFlag(FLAGS_read_percent);
  const int64_t num_keys = absl::GetFlag(FLAGS_num_keys);
  const int batch_size = absl::GetFlag(FLAGS_batch_size);
  const bool sync_writes = absl::GetFlag(FLAGS_sync_writes);

  rocksdb::ReadOptions read_opts;
  rocksdb::WriteOptions write_opts;
  write_opts.sync = sync_writes;

  for (int64_t op = 0; op < ops_count; ++op) {
    int64_t key_id = absl::Uniform(gen, 0, num_keys);
    std::string key = GenerateKey(key_id, key_size);

    if (absl::Uniform(gen, 0, 100) < read_percent) {
      // Read operation
      auto start = std::chrono::high_resolution_clock::now();
      std::string value;
      rocksdb::Status s = db->Get(read_opts, key, &value);
      auto end = std::chrono::high_resolution_clock::now();

      if (s.ok() || s.IsNotFound()) {
        stats->reads_completed.fetch_add(1, std::memory_order_relaxed);
        stats->read_latency_ns.fetch_add(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count(),
            std::memory_order_relaxed);
      } else {
        stats->errors.fetch_add(1, std::memory_order_relaxed);
      }
    } else {
      // Write operation
      auto start = std::chrono::high_resolution_clock::now();
      rocksdb::Status s;

      if (batch_size > 1) {
        rocksdb::WriteBatch batch;
        for (int i = 0; i < batch_size; ++i) {
          int64_t batch_key_id = absl::Uniform(gen, 0, num_keys);
          std::string batch_key = GenerateKey(batch_key_id, key_size);
          std::string value = GenerateValue(gen, value_size);
          batch.Put(batch_key, value);
        }
        s = db->Write(write_opts, &batch);
      } else {
        std::string value = GenerateValue(gen, value_size);
        s = db->Put(write_opts, key, value);
      }
      auto end = std::chrono::high_resolution_clock::now();

      if (s.ok()) {
        stats->writes_completed.fetch_add(batch_size, std::memory_order_relaxed);
        stats->write_latency_ns.fetch_add(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count(),
            std::memory_order_relaxed);
      } else {
        stats->errors.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }
}

// Pre-populate the database with initial data
void PopulateDatabase(rocksdb::DB* db, int64_t num_keys, int key_size,
                      int value_size) {
  std::cout << "Populating database with " << num_keys << " keys..." << std::endl;

  absl::BitGen gen;
  rocksdb::WriteOptions write_opts;
  write_opts.sync = false;

  const int batch_size = 1000;
  rocksdb::WriteBatch batch;
  int64_t keys_written = 0;

  for (int64_t i = 0; i < num_keys; ++i) {
    std::string key = GenerateKey(i, key_size);
    std::string value = GenerateValue(gen, value_size);
    batch.Put(key, value);

    if ((i + 1) % batch_size == 0 || i == num_keys - 1) {
      rocksdb::Status s = db->Write(write_opts, &batch);
      if (!s.ok()) {
        std::cerr << "Error populating database: " << s.ToString() << std::endl;
        return;
      }
      batch.Clear();
      keys_written = i + 1;

      if (keys_written % 100000 == 0) {
        std::cout << "  Written " << keys_written << " keys..." << std::endl;
      }
    }
  }

  // Force a flush to ensure data is on disk
  db->Flush(rocksdb::FlushOptions());
  std::cout << "Database populated with " << keys_written << " keys." << std::endl;
}

void PrintConfiguration() {
  std::cout << "\n========== RocksDB Benchmark Configuration ==========\n";
  std::cout << "Duration:            ";
  if (absl::GetFlag(FLAGS_duration_secs) > 0) {
    std::cout << absl::GetFlag(FLAGS_duration_secs) << " seconds (time-based)\n";
  } else {
    std::cout << absl::GetFlag(FLAGS_ops_per_thread) << " ops/thread (ops-based)\n";
  }
  std::cout << "Threads:             " << absl::GetFlag(FLAGS_num_threads) << "\n";
  std::cout << "Batch size:          " << absl::GetFlag(FLAGS_batch_size) << "\n";
  std::cout << "Read percent:        " << absl::GetFlag(FLAGS_read_percent) << "%\n";
  std::cout << "Key size:            " << absl::GetFlag(FLAGS_key_size) << " bytes\n";
  std::cout << "Value size:          " << absl::GetFlag(FLAGS_value_size) << " bytes\n";
  std::cout << "Key space:           " << absl::GetFlag(FLAGS_num_keys) << " keys\n";
  std::cout << "DB path:             " << absl::GetFlag(FLAGS_db_path) << "\n";
  std::cout << "Use existing DB:     " << (absl::GetFlag(FLAGS_use_existing_db) ? "yes" : "no") << "\n";
  std::cout << "Compression:         " << (absl::GetFlag(FLAGS_enable_compression) ? "enabled" : "disabled") << "\n";
  std::cout << "Sync writes:         " << (absl::GetFlag(FLAGS_sync_writes) ? "yes" : "no") << "\n";
  std::cout << "Write buffer size:   " << absl::GetFlag(FLAGS_write_buffer_size_mb) << " MB\n";
  std::cout << "=====================================================\n\n";
}

void PrintResults(const BenchmarkStats& stats, absl::Duration elapsed) {
  double elapsed_secs = absl::ToDoubleSeconds(elapsed);
  int64_t total_reads = stats.reads_completed.load();
  int64_t total_writes = stats.writes_completed.load();
  int64_t total_ops = total_reads + total_writes;
  int64_t errors = stats.errors.load();

  double read_latency_avg_us = total_reads > 0
      ? static_cast<double>(stats.read_latency_ns.load()) / total_reads / 1000.0
      : 0.0;
  double write_latency_avg_us = total_writes > 0
      ? static_cast<double>(stats.write_latency_ns.load()) / total_writes / 1000.0
      : 0.0;

  std::cout << "\n==================== Benchmark Results ====================\n";
  std::cout << absl::StrFormat("Elapsed time:        %.2f seconds\n", elapsed_secs);
  std::cout << absl::StrFormat("Total operations:    %lld\n", total_ops);
  std::cout << absl::StrFormat("  Reads:             %lld (%.1f%%)\n",
                               total_reads,
                               total_ops > 0 ? 100.0 * total_reads / total_ops : 0.0);
  std::cout << absl::StrFormat("  Writes:            %lld (%.1f%%)\n",
                               total_writes,
                               total_ops > 0 ? 100.0 * total_writes / total_ops : 0.0);
  std::cout << absl::StrFormat("Errors:              %lld\n", errors);
  std::cout << "\n";
  std::cout << absl::StrFormat("Throughput:          %.2f ops/sec\n",
                               total_ops / elapsed_secs);
  std::cout << absl::StrFormat("  Read throughput:   %.2f ops/sec\n",
                               total_reads / elapsed_secs);
  std::cout << absl::StrFormat("  Write throughput:  %.2f ops/sec\n",
                               total_writes / elapsed_secs);
  std::cout << "\n";
  std::cout << absl::StrFormat("Avg read latency:    %.2f us\n", read_latency_avg_us);
  std::cout << absl::StrFormat("Avg write latency:   %.2f us\n", write_latency_avg_us);
  std::cout << "============================================================\n";
}

}  // namespace

int main(int argc, char* argv[]) {
  std::cout << "Starting benchmark..." << std::endl;
  //absl::ParseCommandLine(argc, argv);
  std::cout << "Parsed command line..." << std::endl;

  PrintConfiguration();
  std::cout << "Printed configuration..." << std::endl;
  // Clean up existing database if not using existing
  if (!absl::GetFlag(FLAGS_use_existing_db)) {
    std::string db_path = absl::GetFlag(FLAGS_db_path);
    rocksdb::DestroyDB(db_path, rocksdb::Options());
  }

  // Configure RocksDB
  rocksdb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = absl::GetFlag(FLAGS_write_buffer_size_mb) * 1024 * 1024;
  options.max_write_buffer_number = absl::GetFlag(FLAGS_max_write_buffer_number);
  options.max_background_jobs = absl::GetFlag(FLAGS_num_threads);

  if (!absl::GetFlag(FLAGS_enable_compression)) {
    options.compression = rocksdb::kNoCompression;
  }

  // Open database
  rocksdb::DB* db_raw;
  rocksdb::Status status = rocksdb::DB::Open(options, absl::GetFlag(FLAGS_db_path), &db_raw);
  if (!status.ok()) {
    std::cerr << "Failed to open database: " << status.ToString() << std::endl;
    return 1;
  }
  std::unique_ptr<rocksdb::DB> db(db_raw);

  // Pre-populate if not using existing database
  if (!absl::GetFlag(FLAGS_use_existing_db)) {
    // Populate with a subset of keys for the benchmark
    int64_t populate_keys = std::min(absl::GetFlag(FLAGS_num_keys),
                                     static_cast<int64_t>(100000));
    PopulateDatabase(db.get(), populate_keys,
                     absl::GetFlag(FLAGS_key_size),
                     absl::GetFlag(FLAGS_value_size));
  }

  BenchmarkStats stats;
  std::atomic<bool> stop_flag{false};
  std::vector<std::thread> threads;

  int num_threads = absl::GetFlag(FLAGS_num_threads);
  int duration_secs = absl::GetFlag(FLAGS_duration_secs);
  int64_t ops_per_thread = absl::GetFlag(FLAGS_ops_per_thread);

  std::cout << "Starting benchmark with " << num_threads << " threads...\n";
  auto start_time = absl::Now();

  if (duration_secs > 0) {
    // Time-based benchmark
    for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back(WorkerThreadTimeBased, db.get(), i,
                           absl::Seconds(duration_secs), &stats, &stop_flag);
    }

    // Wait for duration
    absl::SleepFor(absl::Seconds(duration_secs));
    stop_flag.store(true, std::memory_order_relaxed);

    // Wait for threads to finish
    for (auto& t : threads) {
      t.join();
    }
  } else {
    // Operations-based benchmark
    for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back(WorkerThreadOpsBased, db.get(), i,
                           ops_per_thread, &stats);
    }

    // Wait for all threads to complete
    for (auto& t : threads) {
      t.join();
    }
  }

  auto end_time = absl::Now();
  absl::Duration elapsed = end_time - start_time;

  PrintResults(stats, elapsed);

  return 0;
}

