#include <iostream>
#include <cstddef>
#include "rocksdb/db.h"


int main() {
    std::cout << "Standard Alignment: " << alignof(std::max_align_t) << '\n';

    double *ptr = (double*) malloc(sizeof(double));
    std::cout << "Double Alignment: " << alignof(*ptr) << '\n';

    char *ptr2 = (char*) malloc(1);
    std::cout << "Char Alignment: " << alignof(*ptr2) << '\n';

    void *ptr3;
    std::cout << "Sizeof void*: " << sizeof(ptr3) << '\n';

    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status =
    rocksdb::DB::Open(options, "/tmp/testdb", &db);
    assert(status.ok());

    std::string value;
    std::string key1 = "1";
    std::string key2 = "2";
    rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key1, "hello");
    db->Get(rocksdb::ReadOptions(), key1, &value);
    if (s.ok()) s = db->Put(rocksdb::WriteOptions(), key2, value);
    if (s.ok()) s = db->Delete(rocksdb::WriteOptions(), key1);
    s = db->Get(rocksdb::ReadOptions(), key1, &value);
    if (s.ok()) s = db->Put(rocksdb::WriteOptions(), key2, value);
    if (s.ok()) s = db->Delete(rocksdb::WriteOptions(), key1);
    s = db->Get(rocksdb::ReadOptions(), key1, &value);
    if (s.ok()) s = db->Put(rocksdb::WriteOptions(), key2, value);
    if (s.ok()) s = db->Delete(rocksdb::WriteOptions(), key1);
    s = db->Get(rocksdb::ReadOptions(), key1, &value);
    if (s.ok()) s = db->Put(rocksdb::WriteOptions(), key2, value);
    if (s.ok()) s = db->Delete(rocksdb::WriteOptions(), key1);
    s = db->Get(rocksdb::ReadOptions(), key1, &value);
    if (s.ok()) s = db->Put(rocksdb::WriteOptions(), key2, value);
    if (s.ok()) s = db->Delete(rocksdb::WriteOptions(), key1);
    std::string s2;
    db->Get(rocksdb::ReadOptions(), key2, &s2);
    std::cout << "Here: " << s2 << std::endl;
    delete db;
    return 0;
}
