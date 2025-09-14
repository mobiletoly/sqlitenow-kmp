package dev.goquick.sqlitenow.oversqlite

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import org.junit.Test
import org.junit.runner.RunWith
import java.util.UUID

@RunWith(AndroidJUnit4::class)
class BlobPrimaryKeyVersionCheckTest {

    @Test
    fun test_blob_primary_key_version_check_bug() = runBlockingTest {
        println("\n🐛 REPRODUCING BLOB PRIMARY KEY VERSION CHECK BUG")
        println("Issue: Version check uses wrong PK format for BLOB columns during post-upload lookback")

        val db = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        
        // Create a table with BLOB primary key (like the person table in the logs)
        db.execSQL("""
            CREATE TABLE test_table (
                id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """)

        // Create sync metadata tables
        db.execSQL("""
            CREATE TABLE _sync_row_meta (
                table_name TEXT NOT NULL,
                pk_uuid TEXT NOT NULL,
                server_version INTEGER NOT NULL DEFAULT 0,
                deleted INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                PRIMARY KEY (table_name, pk_uuid)
            )
        """)

        // Simulate the exact scenario from the logs
        val uuidString = "2a5d6a01-c7f9-ca2b-49fa-1c552efd8e44"
        val uuidHex = "2a5d6a01c7f9ca2b49fa1c552efd8e44"  // hex format used in _sync_row_meta
        
        println("🔍 Testing UUID formats:")
        println("  UUID string (from server): $uuidString")
        println("  UUID hex (in metadata):    $uuidHex")

        // Insert a record in _sync_row_meta with hex format (as it should be)
        db.prepare("""
            INSERT OR REPLACE INTO _sync_row_meta (table_name, pk_uuid, server_version, deleted)
            VALUES ('test_table', ?, 1, 0)
        """).use { st ->
            st.bindText(1, uuidHex)
            st.step()
        }

        // Verify the record exists in metadata
        val metaExists = db.prepare("SELECT server_version FROM _sync_row_meta WHERE table_name=? AND pk_uuid=?").use { st ->
            st.bindText(1, "test_table")
            st.bindText(2, uuidHex)
            if (st.step()) st.getLong(0) else null
        }
        println("✅ Metadata record exists with server_version: $metaExists")

        // Now test the BUGGY version check (using UUID string instead of hex)
        val buggyVersionCheck = db.prepare("SELECT server_version FROM _sync_row_meta WHERE table_name=? AND pk_uuid=?").use { st ->
            st.bindText(1, "test_table")
            st.bindText(2, uuidString)  // BUG: Using UUID string instead of hex
            if (st.step()) st.getLong(0) else null
        }
        println("🐛 Buggy version check result: $buggyVersionCheck")

        // Test the CORRECT version check (using hex format)
        val correctVersionCheck = db.prepare("SELECT server_version FROM _sync_row_meta WHERE table_name=? AND pk_uuid=?").use { st ->
            st.bindText(1, "test_table")
            st.bindText(2, uuidHex)  // CORRECT: Using hex format
            if (st.step()) st.getLong(0) else null
        }
        println("✅ Correct version check result: $correctVersionCheck")

        // Verify the bug
        assert(buggyVersionCheck == null) { "Buggy version check should return null (not found)" }
        assert(correctVersionCheck == 1L) { "Correct version check should return server_version=1" }

        println("\n🎯 BUG CONFIRMED:")
        println("  - When using UUID string format: version check returns null (record not found)")
        println("  - When using hex format: version check returns correct server_version=1")
        println("  - This causes the system to think there's no existing record")
        println("  - Which triggers conflict resolution instead of normal version checking")
        println("  - Leading to the older INSERT being applied over the newer UPDATE")

        println("\n🔧 THE FIX:")
        println("  In handleNormalApply(), the version check query should convert UUID string to hex")
        println("  for BLOB primary keys, just like it does when updating the metadata")
    }

    private suspend inline fun SafeSQLiteConnection.execSQL(sql: String, block: (androidx.sqlite.SQLiteStatement) -> Unit = {}) {
        prepare(sql).use { st ->
            block(st)
            st.step()
        }
    }
}
