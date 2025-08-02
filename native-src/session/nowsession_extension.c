/*
** SQLite Session Extension for SqliteNow
** Clean, optimized version with higher-level session API
*/

#include "sqlite3ext.h"
#include "sqlite3session.h"
SQLITE_EXTENSION_INIT1

#include <string.h>

/* Platform-specific logging */
#ifdef __ANDROID__
#include <android/log.h>
#define LOG_I(...) __android_log_print(ANDROID_LOG_INFO, "SessionExt", __VA_ARGS__)
#define LOG_E(...) __android_log_print(ANDROID_LOG_ERROR, "SessionExt", __VA_ARGS__)
#elif defined(__APPLE__)
#include <stdio.h>
#define LOG_I(...) do { printf("[INFO] SessionExt: "); printf(__VA_ARGS__); printf("\n"); fflush(stdout); } while(0)
#define LOG_E(...) do { printf("[ERROR] SessionExt: "); printf(__VA_ARGS__); printf("\n"); fflush(stdout); } while(0)
#else
#include <stdio.h>
#define LOG_I(...) do { printf("[INFO] SessionExt: "); printf(__VA_ARGS__); printf("\n"); fflush(stdout); } while(0)
#define LOG_E(...) do { printf("[ERROR] SessionExt: "); printf(__VA_ARGS__); printf("\n"); fflush(stdout); } while(0)
#endif

static sqlite3_session *gSession = NULL;

/* nowsession_begin(tableName, ...) */
static void nowsession_begin(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    /* Clean up any existing session */
    if (gSession) {
        sqlite3session_delete(gSession);
        gSession = NULL;
    }
    /* At least one table name must be provided */
    if (argc < 1) {
        sqlite3_result_error(ctx, "Usage: nowsession_begin(table1[, table2,...])", -1);
        return;
    }
    /* Create session on the main database */
    if (sqlite3session_create(db, "main", &gSession) != SQLITE_OK) {
        sqlite3_result_error(ctx, "Failed to create session", -1);
        return;
    }

    /* Enable the session */
    sqlite3session_enable(gSession, 1);

    /* Attach all specified tables */
    for (int i = 0; i < argc; ++i) {
        const char *table_name = (const char *) sqlite3_value_text(argv[i]);
        if (sqlite3session_attach(gSession, table_name) != SQLITE_OK) {
            sqlite3session_delete(gSession);
            gSession = NULL;
            sqlite3_result_error(ctx, "Failed to attach session to table", -1);
            return;
        }
    }
    sqlite3_result_text(ctx, "OK", -1, SQLITE_STATIC);
}

/* nowsession_attach(tableName) */
static void nowsession_attach(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (!gSession) {
        sqlite3_result_error(ctx, "No active session. Call nowsession_begin first.", -1);
        return;
    }
    if (argc != 1) {
        sqlite3_result_error(ctx, "Usage: nowsession_attach(tableName)", -1);
        return;
    }
    const char *tbl = (const char *) sqlite3_value_text(argv[0]);
    if (sqlite3session_attach(gSession, tbl) != SQLITE_OK) {
        sqlite3_result_error(ctx, "Failed to attach session to table", -1);
        return;
    }
    sqlite3_result_text(ctx, "OK", -1, SQLITE_STATIC);
}

/* nowsession_changeset() -> BLOB */
static void nowsession_changeset(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (!gSession) {
        sqlite3_result_error(ctx, "No active session. Call nowsession_begin first.", -1);
        return;
    }
    int n = 0;
    void *p = NULL;
    if (sqlite3session_changeset(gSession, &n, &p) != SQLITE_OK) {
        sqlite3_result_error(ctx, "Failed to generate changeset", -1);
        return;
    }
    sqlite3_result_blob(ctx, p, n, sqlite3_free);
}

/* nowsession_end() -> TEXT */
static void nowsession_end(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (!gSession) {
        sqlite3_result_error(ctx, "No active session.", -1);
        return;
    }
    sqlite3session_delete(gSession);
    gSession = NULL;
    sqlite3_result_text(ctx, "OK", -1, SQLITE_STATIC);
}

static void nowsession_ping(sqlite3_context *context, int argc, sqlite3_value **argv) {
    /* Check that exactly one argument is provided */
    if (argc != 1) {
        sqlite3_result_error(context, "nowsession_ping() requires exactly one integer argument", -1);
        return;
    }

    /* Get the integer value from the first argument */
    int input_value = sqlite3_value_int(argv[0]);

    LOG_I("nowsession_ping called with value: %d", input_value);

    /* Return the same integer value back */
    sqlite3_result_int(context, input_value);
}

int sqlite3_nowsession_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);

    struct {
        const char *name;
        int nArg;

        void (*func)(sqlite3_context *, int, sqlite3_value **);
    } funcs[] = {
                {"nowsession_ping", 1, nowsession_ping},
                {"nowsession_begin", -1, nowsession_begin}, // -1 means variable arguments (at least 1)
                {"nowsession_attach", 1, nowsession_attach},
                {"nowsession_changeset", 0, nowsession_changeset},
                {"nowsession_end", 0, nowsession_end}
            };

    int rc = SQLITE_OK;
    for (int i = 0; i < sizeof(funcs) / sizeof(funcs[0]); i++) {
        LOG_I("Registering function: %s", funcs[i].name);
        rc = sqlite3_create_function(db, funcs[i].name, funcs[i].nArg, SQLITE_UTF8, 0, funcs[i].func, 0, 0);
        if (rc != SQLITE_OK) {
            LOG_E("Failed to register %s: %d", funcs[i].name, rc);
            if (pzErrMsg) {
                *pzErrMsg = sqlite3_mprintf("Failed to register %s: %d", funcs[i].name, rc);
            }
            return rc;
        }
        LOG_I("Successfully registered: %s", funcs[i].name);
    }

    LOG_I("----> Session extension loaded successfully with %d functions", (int)(sizeof(funcs)/sizeof(funcs[0])));
    return rc;
}

/* Alternative entry points */
int sqlite3_nowsessionext_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    return sqlite3_nowsession_init(db, pzErrMsg, pApi);
}

int sqlite3_libnowsession_ext_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    return sqlite3_nowsession_init(db, pzErrMsg, pApi);
}
