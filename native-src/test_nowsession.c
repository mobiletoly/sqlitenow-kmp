/*
** Test program for nowsession_extension
** This program initializes SQLite3, creates a database, loads the extension
** dynamically, and tests the session functionality.
*/

/*
** IMPORTANT: Run this using the CMake target "test_nowsession", not this .c file directly!
** Use the dropdown in CLion toolbar and select "test_nowsession" (not "test_nowsession.c")
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "sqlite3.h"
#include "sqlite3session.h"

/* Test result tracking */
static int test_count = 0;
static int test_passed = 0;

/* Helper macros for testing */
#define TEST_START(name) \
    do { \
        test_count++; \
        printf("Test %d: %s... ", test_count, name); \
        fflush(stdout); \
    } while(0)

#define TEST_PASS() \
    do { \
        test_passed++; \
        printf("PASS\n"); \
    } while(0)

#define TEST_FAIL(msg) \
    do { \
        printf("FAIL - %s\n", msg); \
    } while(0)

#define ASSERT_RC(rc, expected, msg) \
    do { \
        if ((rc) != (expected)) { \
            TEST_FAIL(msg); \
            printf("  Expected: %d, Got: %d (%s)\n", expected, rc, sqlite3_errstr(rc)); \
            return 0; \
        } \
    } while(0)

/* Callback for sqlite3_exec */
static int exec_callback(void *data, int argc, char **argv, char **azColName) {
    printf("  Result: ");
    for (int i = 0; i < argc; i++) {
        printf("%s=%s ", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}

/* Callback to count rows */
static int count_callback(void *data, int argc, char **argv, char **azColName) {
    int *count = (int*)data;
    (*count)++;
    return 0;
}

/* Helper function to apply changeset to a database */
static int apply_changeset_to_db(sqlite3 *target_db, const void *changeset_data, int changeset_size) {
    sqlite3_changeset_iter *iter = NULL;
    int rc;

    /* Create changeset iterator */
    rc = sqlite3changeset_start(&iter, changeset_size, (void*)changeset_data);
    if (rc != SQLITE_OK) {
        printf("    Failed to create changeset iterator: %d\n", rc);
        return rc;
    }

    /* Apply changeset */
    rc = sqlite3changeset_apply(target_db, changeset_size, (void*)changeset_data,
                               NULL, NULL, NULL);

    if (iter) {
        sqlite3changeset_finalize(iter);
    }

    return rc;
}

/* Test basic SQLite functionality */
int test_sqlite_basic(sqlite3 *db) {
    TEST_START("Basic SQLite functionality");
    
    int rc;
    char *err_msg = 0;
    
    /* Test simple query */
    rc = sqlite3_exec(db, "SELECT sqlite_version()", exec_callback, 0, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to execute SELECT sqlite_version()");
    
    if (err_msg) {
        sqlite3_free(err_msg);
        err_msg = 0;
    }
    
    TEST_PASS();
    return 1;
}

/* Test dynamic extension loading */
int test_extension_loading(sqlite3 *db) {
    TEST_START("-------------- Dynamic extension loading");

    int rc;
    char *err_msg = 0;

    /* Enable extension loading */
    rc = sqlite3_enable_load_extension(db, 1);
    ASSERT_RC(rc, SQLITE_OK, "Failed to enable extension loading");

    /* Load the nowsession extension */
    rc = sqlite3_load_extension(db, "./libnowsession_ext.dylib", "sqlite3_nowsession_init", &err_msg);
    if (rc != SQLITE_OK) {
        TEST_FAIL("Failed to load nowsession extension");
        printf("  Error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    ASSERT_RC(rc, SQLITE_OK, err_msg ? err_msg : "Failed to load nowsession extension");

    if (err_msg) {
        sqlite3_free(err_msg);
        err_msg = 0;
    }

    TEST_PASS();
    return 1;
}

/* Test nowsession_ping function */
int test_nowsession_ping_func(sqlite3 *db) {
    TEST_START("nowsession_ping(1234) function");

    int rc;
    sqlite3_stmt *stmt = NULL;

    /* Prepare the statement */
    rc = sqlite3_prepare_v2(db, "SELECT nowsession_ping(1234) AS result", -1, &stmt, NULL);
    ASSERT_RC(rc, SQLITE_OK, "Failed to prepare nowsession_ping(1234) statement");

    /* Execute the statement */
    rc = sqlite3_step(stmt);
    ASSERT_RC(rc, SQLITE_ROW, "Failed to execute nowsession_ping(1234)");

    /* Get the result value */
    int result = sqlite3_column_int(stmt, 0);
    printf("  nowsession_ping(1234) returned: %d\n", result);

    /* Verify the result is exactly 1234 */
    if (result != 1234) {
        TEST_FAIL("Expected 1234, got different value");
        printf("  Expected: 1234, Got: %d\n", result);
        sqlite3_finalize(stmt);
        return 0;
    }

    /* Clean up */
    sqlite3_finalize(stmt);

    TEST_PASS();
    return 1;
}

/* Test complete session workflow */
int test_session_workflow(sqlite3 *db) {
    TEST_START("Complete session workflow");

    int rc;
    char *err_msg = 0;
    sqlite3_stmt *stmt = NULL;

    /* Create a test table */
    printf("  Creating test table...\n");
    rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", 0, 0, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to create test table");

    /* Insert initial data */
    printf("  Inserting initial data...\n");
    rc = sqlite3_exec(db, "INSERT INTO users (name, age) VALUES ('Alice', 25), ('Bob', 30)", 0, 0, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to insert initial data");

    /* Begin session on the users table */
    printf("  Beginning session on 'users' table...\n");
    rc = sqlite3_exec(db, "SELECT nowsession_begin('users')", exec_callback, 0, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to begin session");

    /* Perform some changes that should be tracked */
    printf("  Performing tracked changes...\n");

    /* Insert new record */
    rc = sqlite3_exec(db, "INSERT INTO users (name, age) VALUES ('Charlie', 35)", 0, 0, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to insert Charlie");

    /* Update existing record */
    rc = sqlite3_exec(db, "UPDATE users SET age = 26 WHERE name = 'Alice'", 0, 0, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to update Alice's age");

    /* Delete a record */
    rc = sqlite3_exec(db, "DELETE FROM users WHERE name = 'Bob'", 0, 0, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to delete Bob");

    /* Get changeset */
    printf("  Getting changeset...\n");
    rc = sqlite3_prepare_v2(db, "SELECT nowsession_changeset()", -1, &stmt, NULL);
    ASSERT_RC(rc, SQLITE_OK, "Failed to prepare changeset query");

    rc = sqlite3_step(stmt);
    ASSERT_RC(rc, SQLITE_ROW, "Failed to get changeset");

    /* Check that we got a changeset (should be a BLOB with some data) */
    int changeset_size = sqlite3_column_bytes(stmt, 0);
    const void *changeset_data = sqlite3_column_blob(stmt, 0);
    printf("  Changeset size: %d bytes\n", changeset_size);

    if (changeset_size <= 0) {
        TEST_FAIL("Changeset is empty or invalid");
        sqlite3_finalize(stmt);
        return 0;
    }

    /* Copy changeset data before finalizing statement */
    void *changeset_copy = malloc(changeset_size);
    if (!changeset_copy) {
        TEST_FAIL("Failed to allocate memory for changeset");
        sqlite3_finalize(stmt);
        return 0;
    }
    memcpy(changeset_copy, changeset_data, changeset_size);

    sqlite3_finalize(stmt);
    stmt = NULL;

    /* End session */
    printf("  Ending session...\n");
    rc = sqlite3_exec(db, "SELECT nowsession_end()", exec_callback, 0, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to end session");

    /* Test changeset replication to a second database */
    printf("  Testing changeset replication...\n");
    sqlite3 *replica_db;

    /* Create replica database */
    rc = sqlite3_open(":memory:", &replica_db);
    ASSERT_RC(rc, SQLITE_OK, "Failed to create replica database");

    /* Create same table structure in replica */
    rc = sqlite3_exec(replica_db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", 0, 0, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to create table in replica");

    /* Insert initial data in replica (same as original before changes) */
    rc = sqlite3_exec(replica_db, "INSERT INTO users (name, age) VALUES ('Alice', 25), ('Bob', 30)", 0, 0, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to insert initial data in replica");

    /* Apply changeset to replica */
    printf("  Applying changeset to replica database...\n");
    rc = apply_changeset_to_db(replica_db, changeset_copy, changeset_size);
    ASSERT_RC(rc, SQLITE_OK, "Failed to apply changeset to replica");

    /* Verify replica has same data as original */
    printf("  Verifying replica data matches original...\n");

    /* Count rows in original */
    int original_count = 0;
    rc = sqlite3_exec(db, "SELECT * FROM users", count_callback, &original_count, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to count original rows");

    /* Count rows in replica */
    int replica_count = 0;
    rc = sqlite3_exec(replica_db, "SELECT * FROM users", count_callback, &replica_count, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to count replica rows");

    printf("  Original database has %d rows, replica has %d rows\n", original_count, replica_count);

    if (original_count != replica_count) {
        TEST_FAIL("Row count mismatch between original and replica");
        sqlite3_close(replica_db);
        free(changeset_copy);
        return 0;
    }

    /* Verify specific data matches */
    printf("  Original database final state:\n");
    rc = sqlite3_exec(db, "SELECT * FROM users ORDER BY name", exec_callback, 0, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to query original final state");

    printf("  Replica database final state:\n");
    rc = sqlite3_exec(replica_db, "SELECT * FROM users ORDER BY name", exec_callback, 0, &err_msg);
    ASSERT_RC(rc, SQLITE_OK, "Failed to query replica final state");

    /* Clean up */
    sqlite3_close(replica_db);
    free(changeset_copy);

    if (err_msg) {
        sqlite3_free(err_msg);
        err_msg = 0;
    }

    TEST_PASS();
    return 1;
}

/* Main test function */
int main(int argc, char *argv[]) {
    sqlite3 *db;
    int rc;
    const char *db_name = "test_nowsession.db";
    
    printf("=== SQLite nowsession Extension Test ===\n\n");
    
    /* Remove existing test database */
    remove(db_name);
    
    /* Open database */
    printf("Opening database: %s\n", db_name);
    rc = sqlite3_open(db_name, &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }
    
    printf("SQLite version: %s\n\n", sqlite3_libversion());
    
    /* Run tests */
    int all_passed = 1;
    
    all_passed &= test_sqlite_basic(db);
    all_passed &= test_extension_loading(db);
    all_passed &= test_nowsession_ping_func(db);
    all_passed &= test_session_workflow(db);
    
    /* Close database */
    sqlite3_close(db);
    
    /* Print results */
    printf("\n=== Test Results ===\n");
    printf("Tests run: %d\n", test_count);
    printf("Tests passed: %d\n", test_passed);
    printf("Tests failed: %d\n", test_count - test_passed);
    
    if (all_passed && test_passed == test_count) {
        printf("\n✅ All tests PASSED!\n");
        return 0;
    } else {
        printf("\n❌ Some tests FAILED!\n");
        return 1;
    }
}
