# SQLite3 Benchmark

A SQLcipher benchmark tool.

Based on [sqlite-bench](https://github.com/ukontainer/sqlite-bench).

## Before Building

You need to compile [SQLCipher](https://github.com/sqlcipher/sqlcipher) in your machine.

Use the following command to confirm sqlcipher.

```sh
$ sqlcipher --version
```

## Building

```sh
$ make
```

## Usage

```
$ ./sqlite-bench --help
Usage: ./sqlite-bench [OPTION]...
SQLite3 benchmark tool
[OPTION]
  --benchmarks=[BENCH]          specify benchmark
  --compression_ratio=DOUBLE    compression ratio
  --use_existing_db={0,1}       use existing database, must be true if use overwrite or read bench
  --num=INT                     number of entries
  --reads=INT                   number of reads
  --value_size=INT              value size
  --no_transaction              disable transaction
  --page_size=INT               page size
  --num_pages=INT               number of pages
  --WAL_enabled={0,1}           enable WAL
  --use_sqlcipher={0,1}         use sqlcipher
  --key=KEY                     key of sqlcipher, must be set if use sqlcipher
  --db=PATH                     path of the existing database to location databases are created
  --help                        show this help

[BENCH]
  fillseq       write N values in sequential key order in async mode
  fillseqsync   write N values in sequential key order in sync mode
  fillseqbatch  batch write N values in sequential key order in async mode
  fillrandom    write N values in random key order in async mode
  fillrandsync  write N values in random key order in sync mode
  fillrandbatch batch write N values in random key order in async mode
  overwrite     overwrite N values in random key order in async mode
  fillrand100K  write N/1000 100K values in random order in async mode
  fillseq100K   wirte N/1000 100K values in sequential order in async mode
  readseq       read N times sequentially
  readrandom    read N times in random order
  readrand100K  read N/1000 100K values in random order in async mode
```
