local_repository(
  # Name of the TCMalloc repository. This name is defined within your
  # WORKSPACE file, in its `workspace()` metadata
  name = "com_google_tcmalloc",

  # NOTE: Bazel paths must be absolute paths. E.g., you can't use ~/Source
  path = "/users/zyqiao/troubridge",
)

http_archive(
    name = "com_google_absl",
    urls = ["https://github.com/abseil/abseil-cpp/archive/7599e36e7cbad38ec77cadd959d3a45d2124800a.zip"],
    strip_prefix = "abseil-cpp-7599e36e7cbad38ec77cadd959d3a45d2124800a",
    sha256 = "5b1cca11b5389e25cd38fbcd2d115995d7f7a64c13d1d33e8c6a7f8a45f340f2",
)
