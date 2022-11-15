// Copyright (c 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "bench.h"

enum Order {
  SEQUENTIAL,
  RANDOM
};

sqlite3* db_;
int num_;
int reads_;
double op_total_time_;
int64_t bytes_;
char* message_;
RandomGenerator gen_;
Random rand_;

/* State kept for progress messages */
int done_;
int next_report_;

static void print_header(void);
static void print_warnings(void);
static void print_environment(void);
static void start(void);
static void stop(const char *name);

inline
static void exec_error_check(int status, char *err_msg) {
  if (status != SQLITE_OK) {
    fprintf(stderr, "SQL error: %s\n", err_msg);
    sqlite3_free(err_msg);
    exit(1);
  }
}

inline
static void step_error_check(int status) {
  if (status != SQLITE_DONE) {
    fprintf(stderr, "SQL step error: status = %d\n", status);
    exit(1);
  }
}

inline
static void error_check(int status) {
  if (status != SQLITE_OK) {
    fprintf(stderr, "sqlite3 error: status = %d\n", status);
    exit(1);
  }
}

inline
static void wal_checkpoint(sqlite3* db_) {
  /* Flush all writes to disk */
  if (FLAGS_WAL_enabled) {
    sqlite3_wal_checkpoint_v2(db_, NULL, SQLITE_CHECKPOINT_FULL, NULL,
                              NULL);
  }
}

static void print_header() {
  const int kKeySize = 16;
  print_environment();
  fprintf(stderr, "Keys:       %d bytes each\n", kKeySize);
  fprintf(stderr, "Values:     %d bytes each\n", FLAGS_value_size);  
  fprintf(stderr, "Entries:    %d\n", num_);
  fprintf(stderr, "RawSize:    %.1f MB (estimated)\n",
            (((int64_t)(kKeySize + FLAGS_value_size) * num_)
            / 1048576.0));
  print_warnings();
  fprintf(stderr, "------------------------------------------------\n");
}

static void print_warnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
  fprintf(stderr,
      "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
      );
#endif
#ifndef NDEBUG
  fprintf(stderr,
      "WARNING: Assertions are enabled: benchmarks unnecessarily slow\n"
      );
#endif
}

static void print_environment() {
  fprintf(stderr, "SQLite:     version %s\n", SQLITE_VERSION);
#if defined(__linux)
  time_t now = time(NULL);
  fprintf(stderr, "Date:       %s", ctime(&now));

  FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
  if (cpuinfo != NULL) {
    char line[1000];
    int num_cpus = 0;
    char* cpu_type = malloc(sizeof(char) * 1000);
    char* cache_size = malloc(sizeof(char) * 1000);
    while (fgets(line, sizeof(line), cpuinfo) != NULL) {
      char* sep = strchr(line, ':');
      if (sep == NULL) {
        continue;
      }
      char* key = calloc(sizeof(char), 1000);
      char* val = calloc(sizeof(char), 1000);
      strncpy(key, line, sep - 1 - line);
      strcpy(val, sep + 1);
      char* trimed_key = trim_space(key);
      char* trimed_val = trim_space(val);
      free(key);
      free(val);
      if (!strcmp(trimed_key, "model name")) {
        ++num_cpus;
        strcpy(cpu_type, trimed_val);
      } else if (!strcmp(trimed_key, "cache size")) {
        strcpy(cache_size, trimed_val);
      }
      free(trimed_key);
      free(trimed_val);
    }
    fclose(cpuinfo);
    fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type);
    fprintf(stderr, "CPUCache:   %s\n", cache_size);
    free(cpu_type);
    free(cache_size);
  }
#endif
}

static void start() {
  bytes_ = 0;
  message_ = malloc(sizeof(char) * 10000);
  strcpy(message_, "");
  done_ = 0;
  next_report_ = 100;
  op_total_time_ = 0;
}

static void stop(const char* name) {
  if (done_ < 1) done_ = 1;

  if (bytes_ > 0) {
    char *rate = malloc(sizeof(char) * 100);;
    snprintf(rate, strlen(rate), "%6.1f MB/s",
              (bytes_ / 1048576.0) / op_total_time_);
    if (message_ && !strcmp(message_, "")) {
      message_ = strcat(strcat(rate, " "), message_);
    } else {
      message_ = rate;
    }
  }

  fprintf(stderr, "%-12s : %.3f micros/op;\n", name, op_total_time_ / done_);
  fprintf(stderr, "%-12s : %.3f micros in total;\n", name, op_total_time_);
  fflush(stdout);
  fflush(stderr);
}

void finish_single_op() {
  done_++;
  if (done_ >= next_report_) {
    if      (next_report_ < 1000)   next_report_ += 100;
    else if (next_report_ < 5000)   next_report_ += 500;
    else if (next_report_ < 10000)  next_report_ += 1000;
    else if (next_report_ < 50000)  next_report_ += 5000;
    else if (next_report_ < 100000) next_report_ += 10000;
    else if (next_report_ < 500000) next_report_ += 50000;
    else                            next_report_ += 100000;
    fprintf(stderr, "... finished %d ops%30s\r", done_, "");
    fflush(stderr);
  }
}

void gen_key(int* keys, int num, int order) {
  if (order == SEQUENTIAL) {
    for (int i = 0; i < num; ++i) {
      keys[i] = i;
    }
  } else {
    for (int i = 0; i < num; ++i) {
      keys[i] = rand_next(&rand_) % num;
    }
  }
}

void gen_value(char** values, int num, int value_size) {
  for (int i = 0; i < num; ++i) {
    values[i] = rand_gen_generate(&gen_, value_size);;
  }
}

void benchmark_init() {
  db_ = NULL;
  num_ = FLAGS_num;
  reads_ = FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads;
  bytes_ = 0;
  rand_gen_init(&gen_, FLAGS_compression_ratio);
  rand_init(&rand_, time(0));

  struct dirent* ep;
  DIR* test_dir = opendir(FLAGS_db);
  if (!FLAGS_use_existing_db) {
    while ((ep = readdir(test_dir)) != NULL) {
      if (starts_with(ep->d_name, "dbbench_sqlite3")) {
        char file_name[1000];
        strcpy(file_name, FLAGS_db);
        strcat(file_name, ep->d_name);
        remove(file_name);
      }
    }
  }
  closedir(test_dir);
}

void benchmark_fini() {
  int status = sqlite3_close(db_);
  error_check(status);
}

void benchmark_run() {
  print_header();
  benchmark_open();

  char* benchmarks = FLAGS_benchmarks;
  while (benchmarks != NULL) {
    char* sep = strchr(benchmarks, ',');
    char* name;
    if (sep == NULL) {
      name = benchmarks;
      benchmarks = NULL;
    } else {
      name = calloc(sizeof(char), (sep - benchmarks + 1));
      strncpy(name, benchmarks, sep - benchmarks);
      benchmarks = sep + 1;
    }
    if (if_create_database(name)) {
      int status;
      char* err_msg = NULL;
      /* create tables/index for database if bench is not overwrite*/
      char* create_stmt =
              "CREATE TABLE test (key int, value text, PRIMARY KEY (key))";
      status = sqlite3_exec(db_, create_stmt, NULL, NULL, &err_msg);
      exec_error_check(status, err_msg);
    }
    bytes_ = 0;
    start();
    bool known = true;
    bool write_sync = false;
    if (!strcmp(name, "fillseq")) {
      benchmark_write(write_sync, SEQUENTIAL, num_, FLAGS_value_size, 1);
      wal_checkpoint(db_);
    } else if (!strcmp(name, "fillseqbatch")) {
      benchmark_write(write_sync, SEQUENTIAL, num_, FLAGS_value_size, 1000);
      wal_checkpoint(db_);
    } else if (!strcmp(name, "fillrandom")) {
      benchmark_write(write_sync, RANDOM, num_, FLAGS_value_size, 1);
      wal_checkpoint(db_);
    } else if (!strcmp(name, "fillrandbatch")) {
      benchmark_write(write_sync, RANDOM, num_, FLAGS_value_size, 1000);
      wal_checkpoint(db_);
    } else if (!strcmp(name, "overwrite")) {
      benchmark_write(write_sync, RANDOM, num_, FLAGS_value_size, 1);
      wal_checkpoint(db_);
    } else if (!strcmp(name, "overwritesync")) {
	  write_sync = true;
      benchmark_write(write_sync, RANDOM, num_, FLAGS_value_size, 1);
      wal_checkpoint(db_);
    } else if (!strcmp(name, "overwritebatch")) {
      benchmark_write(write_sync, RANDOM, num_, FLAGS_value_size, 1000);
      wal_checkpoint(db_);
    } else if (!strcmp(name, "fillrandsync")) {
      write_sync = true;
      benchmark_write(write_sync, RANDOM, num_, FLAGS_value_size, 1);
      wal_checkpoint(db_);
    } else if (!strcmp(name, "fillseqsync")) {
      write_sync = true;
      benchmark_write(write_sync, SEQUENTIAL, num_, FLAGS_value_size, 1);
      wal_checkpoint(db_);
    } else if (!strcmp(name, "fillrand100K")) {
      benchmark_write(write_sync, RANDOM, num_ / 1000, 100 * 1000, 1);
      wal_checkpoint(db_);
    } else if (!strcmp(name, "fillseq100K")) {
      benchmark_write(write_sync, SEQUENTIAL, num_ / 1000, 100 * 1000, 1);
      wal_checkpoint(db_);
    } else if (!strcmp(name, "readseq")) {
      benchmark_read(SEQUENTIAL, 1);
    } else if (!strcmp(name, "readrandom")) {
      benchmark_read(RANDOM, 1);
    } else if (!strcmp(name, "readrand100K")) {
      int n = reads_;
      reads_ /= 1000;
      benchmark_read(RANDOM, 1);
      reads_ = n;
    } else if (!strcmp(name, "delete")) {
      benchmark_delete(write_sync, RANDOM, 1);
	    wal_checkpoint(db_);
    } else if (!strcmp(name, "deletesync")) {
      write_sync = true;
      benchmark_delete(write_sync, RANDOM, 1);
      wal_checkpoint(db_);
    } else {
      known = false;
      if (strcmp(name, "")) {
        fprintf(stderr, "unknown benchmark '%s'\n", name);
      }
    }
    if (known) {
      stop(name);
    }
  }
}

void benchmark_open() {
  assert(db_ == NULL);

  int status;
  char file_name[100];
  char* err_msg = NULL;

  /* Open database */
  if (FLAGS_use_existing_db) {
    snprintf(file_name, sizeof(file_name),
             "%s",
             FLAGS_db);
  } else {
      snprintf(file_name, sizeof(file_name),
               "%sdbbench_sqlite3.db",
               FLAGS_db);
  }
  status = sqlite3_open(file_name, &db_);
  if (status) {
    fprintf(stderr, "open error: %s\n", sqlite3_errmsg(db_));
    exit(1);
  }

  /* Input Key */
  if(FLAGS_use_sqlcipher) {
    status = sqlite3_key(db_, FLAGS_key, strlen(FLAGS_key));
    if (status) {
      fprintf(stderr, "input key error: %s\n", sqlite3_errmsg(db_));
      exit(1);
    }
  }

  /* Change SQLite cache size */
  char cache_size[100];
  snprintf(cache_size, sizeof(cache_size), "PRAGMA cache_size = %d",
            FLAGS_num_pages);
  status = sqlite3_exec(db_, cache_size, NULL, NULL, &err_msg);
  exec_error_check(status, err_msg);

  /* FLAGS_page_size is defaulted to 1024 */
  if (FLAGS_page_size != 1024) {
    char page_size[100];
    snprintf(page_size, sizeof(page_size), "PRAGMA page_size = %d",
              FLAGS_page_size);
    status = sqlite3_exec(db_, page_size, NULL, NULL, &err_msg);
    exec_error_check(status, err_msg);
  }

  /* Change journal mode to WAL if WAL enabled flag is on */
  if (FLAGS_WAL_enabled) {
    char* WAL_stmt = "PRAGMA journal_mode = WAL";

    /* Default cache size is a combined 4 MB */
    char* WAL_checkpoint = "PRAGMA wal_autocheckpoint = 4096";
    status = sqlite3_exec(db_, WAL_stmt, NULL, NULL, &err_msg);
    exec_error_check(status, err_msg);
    status = sqlite3_exec(db_, WAL_checkpoint, NULL, NULL, &err_msg);
    exec_error_check(status, err_msg);
  }

  /* Change locking mode to exclusive */
  char* locking_stmt = "PRAGMA locking_mode = EXCLUSIVE";
  status = sqlite3_exec(db_, locking_stmt, NULL, NULL, &err_msg);
  exec_error_check(status, err_msg);
}

void benchmark_write(bool write_sync, int order, int num_entries, int value_size, int entries_per_batch) {
  if (num_entries != num_) {
    char* msg = malloc(sizeof(char) * 100);
    snprintf(msg, 100, "(%d ops)", num_entries);
    message_ = msg;
  }

  char* err_msg = NULL;
  int status;

  sqlite3_stmt *replace_stmt, *begin_trans_stmt, *end_trans_stmt;
  char* replace_str = "REPLACE INTO test (key, value) VALUES (?, ?)";
  char *begin_trans_str = "BEGIN TRANSACTION";
  char *end_trans_str = "END TRANSACTION";

  /* Check for synchronous flag in options */
  char* sync_stmt = (write_sync) ? "PRAGMA synchronous = FULL" :
                                    "PRAGMA synchronous = OFF";
  status = sqlite3_exec(db_, sync_stmt, NULL, NULL, &err_msg);
  exec_error_check(status, err_msg);

  /* Preparing sqlite3 statements */
  status = sqlite3_prepare_v2(db_, replace_str, -1,
                              &replace_stmt, NULL);
  error_check(status);
  status = sqlite3_prepare_v2(db_, begin_trans_str, -1,
                              &begin_trans_stmt, NULL);
  error_check(status);
  status = sqlite3_prepare_v2(db_, end_trans_str, -1,
                              &end_trans_stmt, NULL);
  error_check(status);

  /*test MAXNUMPERTIME = 500000 at most*/
  for (int n = num_entries > MAXNUMPERTIME ? MAXNUMPERTIME : num_entries; n > 0; n -= MAXNUMPERTIME) {
    /* Generate keys and values */
    int keys[n];
    gen_key(keys, n, order);
    char* values[n];
    gen_value(values, n, value_size);

    double start = now_micros();

    /* Begin write transaction */
    if (FLAGS_transaction) {
      status = sqlite3_step(begin_trans_stmt);
      step_error_check(status);
      status = sqlite3_reset(begin_trans_stmt);
      error_check(status);
    }

    for (int i = 0; i < n; i += entries_per_batch) {
      /* Create and execute SQL statements */
      for (int j = 0; j < entries_per_batch; j++) {
        /* Bind KV values into replace_stmt */
        status = sqlite3_bind_int(replace_stmt, 1, keys[i+j]);
        error_check(status);
        status = sqlite3_bind_blob(replace_stmt, 2, values[i+j],
                                   value_size, SQLITE_STATIC);
        error_check(status);

        /* Execute replace_stmt */
        bytes_ += value_size + sizeof(int);
        status = sqlite3_step(replace_stmt);
        step_error_check(status);

        /* Reset SQLite statement for another use */
        status = sqlite3_clear_bindings(replace_stmt);
        error_check(status);
        status = sqlite3_reset(replace_stmt);
        error_check(status);

        finish_single_op();
      }
    }

    /* End write transaction */
    if (FLAGS_transaction) {
      status = sqlite3_step(end_trans_stmt);
      step_error_check(status);
      status = sqlite3_reset(end_trans_stmt);
      error_check(status);
    }

    double end = now_micros();
    op_total_time_ += end - start;
  }

  status = sqlite3_finalize(replace_stmt);
  error_check(status);
  status = sqlite3_finalize(begin_trans_stmt);
  error_check(status);
  status = sqlite3_finalize(end_trans_stmt);
  error_check(status);
}

void benchmark_read(int order, int entries_per_batch) {
  int status;
  sqlite3_stmt *read_stmt, *begin_trans_stmt, *end_trans_stmt;

  char *read_str = "SELECT * FROM test WHERE key = ?";
  char *begin_trans_str = "BEGIN TRANSACTION";
  char *end_trans_str = "END TRANSACTION";

  /* Preparing sqlite3 statements */
  status = sqlite3_prepare_v2(db_, begin_trans_str, -1,
                              &begin_trans_stmt, NULL);
  error_check(status);
  status = sqlite3_prepare_v2(db_, end_trans_str, -1,
                              &end_trans_stmt, NULL);
  error_check(status);
  status = sqlite3_prepare_v2(db_, read_str, -1,
                              &read_stmt, NULL);
  error_check(status);

  /*test MAXNUMPERTIME = 500000 at most*/
  for (int n = reads_ > MAXNUMPERTIME ? MAXNUMPERTIME : reads_; n > 0; n -= MAXNUMPERTIME) {
    /* Generate keys */
    int keys[n];
    gen_key(keys, n, order);

    double start = now_micros();

    /* Begin read transaction */
    if (FLAGS_transaction) {
      status = sqlite3_step(begin_trans_stmt);
      step_error_check(status);
      status = sqlite3_reset(begin_trans_stmt);
      error_check(status);
    }
    for (int i = 0; i < n; i += entries_per_batch) {
      /* Create and execute SQL statements */
      for (int j = 0; j < entries_per_batch; j++) {
        /* Bind key value into read_stmt */
        status = sqlite3_bind_int(read_stmt, 1, keys[i + j]);
        error_check(status);

        /* Execute read statement */
        while ((status = sqlite3_step(read_stmt)) == SQLITE_ROW) {}
        step_error_check(status);

        /* Reset SQLite statement for another use */
        status = sqlite3_clear_bindings(read_stmt);
        error_check(status);
        status = sqlite3_reset(read_stmt);
        error_check(status);

        finish_single_op();
      }
    }

    /* End read transaction */
    if (FLAGS_transaction) {
      status = sqlite3_step(end_trans_stmt);
      step_error_check(status);
      status = sqlite3_reset(end_trans_stmt);
      error_check(status);
    }

    double end = now_micros();
    op_total_time_ += end - start;
  }

  status = sqlite3_finalize(read_stmt);
  error_check(status);
  status = sqlite3_finalize(begin_trans_stmt);
  error_check(status);
  status = sqlite3_finalize(end_trans_stmt);
  error_check(status);
}

void benchmark_delete(bool write_sync, int order, int entries_per_batch) {
  int status;
  sqlite3_stmt *delete_stmt, *begin_trans_stmt, *end_trans_stmt;
  
  char* err_msg = NULL;

  char *delete_str = "DELETE FROM test WHERE key = ?";
  char *begin_trans_str = "BEGIN TRANSACTION";
  char *end_trans_str = "END TRANSACTION";
  
  /* Check for synchronous flag in options */
  char* sync_stmt = (write_sync) ? "PRAGMA synchronous = FULL" :
                                    "PRAGMA synchronous = OFF";
  status = sqlite3_exec(db_, sync_stmt, NULL, NULL, &err_msg);
  exec_error_check(status, err_msg);

  /* Preparing sqlite3 statements */
  status = sqlite3_prepare_v2(db_, begin_trans_str, -1,
                              &begin_trans_stmt, NULL);
  error_check(status);
  status = sqlite3_prepare_v2(db_, end_trans_str, -1,
                              &end_trans_stmt, NULL);
  error_check(status);
  status = sqlite3_prepare_v2(db_, delete_str, -1,
                              &delete_stmt, NULL);
  error_check(status);

/*test MAXNUMPERTIME = 500000 at most*/
  for (int n = num_ > MAXNUMPERTIME ? MAXNUMPERTIME : num_; n > 0; n -= MAXNUMPERTIME) {
    /* Generate keys */
    int keys[n];
    gen_key(keys, n, order);

    double start = now_micros();

    /* Begin delete transaction */
    if (FLAGS_transaction) {
      status = sqlite3_step(begin_trans_stmt);
      step_error_check(status);
      status = sqlite3_reset(begin_trans_stmt);
      error_check(status);
    }
    for (int i = 0; i < n; i += entries_per_batch) {
      /* Create and execute SQL statements */
      for (int j = 0; j < entries_per_batch; j++) {
        /* Bind key value into delete_stmt */
        status = sqlite3_bind_int(delete_stmt, 1, keys[i + j]);
        error_check(status);

        /* Execute read statement */
        while ((status = sqlite3_step(delete_stmt)) == SQLITE_ROW) {}
        step_error_check(status);

        /* Reset SQLite statement for another use */
        status = sqlite3_clear_bindings(delete_stmt);
        error_check(status);
        status = sqlite3_reset(delete_stmt);
        error_check(status);

        finish_single_op();
      }
    }

    /* End delete transaction */
    if (FLAGS_transaction) {
      status = sqlite3_step(end_trans_stmt);
      step_error_check(status);
      status = sqlite3_reset(end_trans_stmt);
      error_check(status);
    }

    double end = now_micros();
    op_total_time_ += end - start;
  }

  status = sqlite3_finalize(delete_stmt);
  error_check(status);
  status = sqlite3_finalize(begin_trans_stmt);
  error_check(status);
  status = sqlite3_finalize(end_trans_stmt);
  error_check(status);
}
