This is the data generation module of Troubridge. Create your tests under this repository and set Troubridge as your memory allocator. 

Example usage:
```
bazel build //examples/hello_world
sudo ./bazel-bin/examples/hello_world # the sudo is important! It allows the Troubridge data collector to access the access bits for allocated pages
```
After running, you will see the data in machine_learning_stats.txt in your working directory. This can be directly copy-pasted into the training/test data files of the machine learning model.

The `examples` directory is a lightweight example to follow on how to make your own tests by adding bazel dependencies to external libraries