// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "bench.h"

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//
//   fillseq       -- write N values in sequential key order in async mode
//   fillseqsync   -- write N/100 values in sequential key order in sync mode
//   fillseqbatch  -- batch write N values in sequential key order in async mode
//   fillrandom    -- write N values in random key order in async mode
//   fillrandsync  -- write N/100 values in random key order in sync mode
//   fillrandbatch -- batch write N values in sequential key order in async mode
//   overwrite     -- overwrite N values in random key order in async mode
//   overwritesync -- overwrite N values in random key order in sync mode
//   fillrand100K  -- write N/1000 100K values in random order in async mode
//   fillseq100K   -- write N/1000 100K values in sequential order in async mode
//   readseq       -- read N times sequentially
//   readrandom    -- read N times in random order
//   readrand100K  -- read N/1000 100K values in random order in async mode
//   delete        -- delete N row in sequential key order in async mode
//   deletesync    -- delete N row in sequential key order in sync mode
char* FLAGS_benchmarks;

// Number of key/values to place in database
int FLAGS_num;

// Number of read operations to do.  If negative, do FLAGS_num reads.
int FLAGS_reads;

// Size of each value
int FLAGS_value_size;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
double FLAGS_compression_ratio;

// Page size. Default 1 KB.
int FLAGS_page_size;

// Number of pages.
// Default cache size = FLAGS_page_size * FLAGS_num_pages = 4 MB.
int FLAGS_num_pages;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
bool FLAGS_use_existing_db;

// If true, we allow batch writes to occur
bool FLAGS_transaction;

// If true, we enable Write-Ahead Logging
bool FLAGS_WAL_enabled;

// Use the db with the following name.
char* FLAGS_db;

// If true, you must input the sqlcipher's key
bool FLAGS_use_sqlcipher;

// Use the key to access to sqlcipher
char* FLAGS_key;

void init() {
  // Comma-separated list of operations to run in the specified order
  //   Actual benchmarks:
  //
  //   fillseq       -- write N values in sequential key order in async mode
  //   fillseqsync   -- write N/100 values in sequential key order in sync mode
  //   fillseqbatch  -- batch write N values in sequential key order in async mode
  //   fillrandom    -- write N values in random key order in async mode
  //   fillrandsync  -- write N/100 values in random key order in sync mode
  //   fillrandbatch -- batch write N values in sequential key order in async mode
  //   overwrite     -- overwrite N values in random key order in async mode
  //   overwritesync -- overwrite N values in random key order in sync mode
  //   fillrand100K  -- write N/1000 100K values in random order in async mode
  //   fillseq100K   -- write N/1000 100K values in sequential order in async mode
  //   readseq       -- read N times sequentially
  //   readrandom    -- read N times in random order
  //   readrand100K  -- read N/1000 100K values in random order in async mode
  //   delete        -- delete N row in sequential key order in async mode
  //   deletesync    -- delete N row in sequential key order in sync mode
  FLAGS_benchmarks =
    "fillseq,"
    "fillseqsync,"
    "fillrandom,"
    "fillrandsync,"
    "overwrite,"
    "overwritesync,"
    "readrandom,"
    "readseq,"
    "fillrand100K,"
    "fillseq100K,"
    "readseq,"
    "readrand100K,"
	  "delete,"
	  "deletesync,"
    ;
  FLAGS_num = 1000000;
  FLAGS_reads = -1;
  FLAGS_value_size = 128;
  FLAGS_compression_ratio = 0.5;
  FLAGS_page_size = 1024;
  FLAGS_num_pages = 4096;
  FLAGS_use_existing_db = false;
  FLAGS_transaction = true;
  FLAGS_WAL_enabled = true;
  FLAGS_db = NULL;
  FLAGS_use_sqlcipher = false;
  FLAGS_key = NULL;
}

void print_usage(const char* argv0) {
  fprintf(stderr, "Usage: %s [OPTION]...\n", argv0);
  fprintf(stderr, "SQLcipher benchmark tool\n");
  fprintf(stderr, "[OPTION]\n");
  fprintf(stderr, "  --benchmarks=[BENCH]\t\tspecify benchmark\n");
  fprintf(stderr, "  --compression_ratio=DOUBLE\tcompression ratio\n");
  fprintf(stderr, "  --use_existing_db={0,1}\tuse existing database, must be true if use overwrite or read bench\n");
  fprintf(stderr, "  --num=INT\t\t\tnumber of entries\n");
  fprintf(stderr, "  --reads=INT\t\t\tnumber of reads\n");
  fprintf(stderr, "  --value_size=INT\t\tvalue size\n");
  fprintf(stderr, "  --no_transaction\t\tdisable transaction\n");
  fprintf(stderr, "  --page_size=INT\t\tpage size\n");
  fprintf(stderr, "  --num_pages=INT\t\tnumber of pages\n");
  fprintf(stderr, "  --WAL_enabled={0,1}\t\tenable WAL\n");
  fprintf(stderr, "  --use_sqlcipher={0,1}\t\tuse sqlcipher\n");
  fprintf(stderr, "  --db=PATH\t\t\tpath of the existing database to location databases are created\n");
  fprintf(stderr, "  --key=KEY\t\t\tkey of sqlcipher, must be set if use sqlcipher\n");
  fprintf(stderr, "  --help\t\t\tshow this help\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "[BENCH]\n");
  fprintf(stderr, "  fillseq\twrite N values in sequential key order in async mode\n");
  fprintf(stderr, "  fillseqsync\twrite N values in sequential key order in sync mode\n");
  fprintf(stderr, "  fillseqbatch\tbatch write N values in sequential key order in async mode\n");
  fprintf(stderr, "  fillrandom\twrite N values in random key order in async mode\n");
  fprintf(stderr, "  fillrandsync\twrite N values in random key order in sync mode\n");
  fprintf(stderr, "  fillrandbatch\tbatch write N values in random key order in async mode\n");
  fprintf(stderr, "  overwrite\toverwrite N values in random key order in async mode\n");
  fprintf(stderr, "  overwritesync\toverwrite N values in random key order in sync mode\n");
  fprintf(stderr, "  fillrand100K\twrite N/1000 100K values in random order in async mode\n");
  fprintf(stderr, "  fillseq100K\twirte N/1000 100K values in sequential order in async mode\n");
  fprintf(stderr, "  readseq\tread N times sequentially\n");
  fprintf(stderr, "  readrandom\tread N times in random order\n");
  fprintf(stderr, "  readrand100K\tread N/1000 100K values in random order in async mode\n");
  fprintf(stderr, "  delete\tdelete N row in random order in async mode\n");
  fprintf(stderr, "  deletesync\tdelete N row in random order in sync mode\n");

}

int main(int argc, char** argv) {
  init();

  char* default_db_path = malloc(sizeof(char) * 1024);
  strcpy(default_db_path, "./");

  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    char junk;
    if (starts_with(argv[i], "--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_compression_ratio = d;
    } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_use_existing_db = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (!strcmp(argv[i], "--no_transaction")) {
      FLAGS_transaction = false;
    } else if (sscanf(argv[i], "--page_size=%d%c", &n, &junk) == 1) {
      FLAGS_page_size = n;
    } else if (sscanf(argv[i], "--num_pages=%d%c", &n, &junk) == 1) {
      FLAGS_num_pages = n;
    } else if (sscanf(argv[i], "--WAL_enabled=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_WAL_enabled = n;
    } else if (sscanf(argv[i], "--use_sqlcipher=%d%c", &n, &junk) == 1 &&
              (n == 0 || n == 1)) {
        FLAGS_use_sqlcipher = n;
    } else if (strncmp(argv[i], "--db=", 5) == 0) {
        FLAGS_db = argv[i] + 5;
    } else if (strncmp(argv[i], "--key=", 6) == 0) {
        FLAGS_key = argv[i] + 6;
    } else if (!strcmp(argv[i], "--help")) {
      print_usage(argv[0]);
      exit(0);
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  /* Choose a location for the test database if none given with --db=<path>  */
  if (FLAGS_db == NULL)
      FLAGS_db = default_db_path;

  benchmark_init();
  benchmark_run();
  benchmark_fini();

  return 0;
}

