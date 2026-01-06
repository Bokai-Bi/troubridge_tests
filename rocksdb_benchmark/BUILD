cc_binary(
    name = "rocksdb_benchmark",
    srcs = ["rocksdb_benchmark.cc"],
    malloc = "@tcmalloc//tcmalloc",
    deps = [
        "@rocksdb//:rocksdb",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/flags:parse",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/time",
        "@abseil-cpp//absl/random",
    ],
)

