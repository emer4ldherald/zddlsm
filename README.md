# zddlsm

zddlsm is a ZDD-based index for LSM-trees.

ZDD-based storage is implemented using [SAPPOROBDD](https://github.com/Shin-ichi-Minato/SAPPOROBDD.git).

Supports [zstd](https://github.com/facebook/zstd.git) compression for keys. There are also some hashing methods added (MD5, SHA256).

## Quick start

The following commands will install all required libraries.

```bash
sudo apt update
sudo apt install libzstd-dev libssl-dev
```

Project uses cmake building system. To build this project execute the following commands.

```bash
mkdir build
cd build
cmake ../
cmake --build .
```

Now you can run unit-tests

```bash
./zddlsm_unittests
```

and resource usage test

```bash
./rusage_test [KEYS_BYTE_LEN] [TEST_SIZE] [COMPRESSION_TYPE] [RESULTS_DIR]
```

where `[COMPRESSION_TYPE]` is one of `zstd`, `md5`, `sha256` or `none`.

You can also run python resource usage comparative test.

```bash
python3 rusage_comp_test.py [KEYS_BYTE_LEN] [TEST_SIZE] [RESULTS_DIR]
```
