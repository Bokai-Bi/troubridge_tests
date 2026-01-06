This is the data generation module of Troubridge. Create your tests under this repository and set Troubridge as your memory allocator. 

Example usage:
```
bazel build //examples/hello_world
sudo ./bazel-bin/examples/hello_world # the sudo is important! It allows the Troubridge data collector to access the access bits for allocated pages
```

The `examples` directory is a lightweight example to follow on how to make your own tests by adding bazel dependencies to external libraries