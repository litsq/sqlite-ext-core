#include <sqlite3.h>
#include <stdio.h>
#include <unistd.h>

int main() {
  sqlite3 *db;
  int rc = sqlite3_open(":memory:", &db);
  if (rc) {
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
    return 1;
  }

  sqlite3_enable_load_extension(db, 1);
  char *errmsg = NULL;
  rc = sqlite3_load_extension(db, "./libintegration_ext.so", 0, &errmsg);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "Extension load failed: %s\n", errmsg);
    sqlite3_free(errmsg);
    return 1;
  }

  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db, "SELECT test_counter();", -1, &stmt, NULL);
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  sqlite3_prepare_v2(db, "SELECT test_counter();", -1, &stmt, NULL);
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  sqlite3_close(db);
  return 0;
}
