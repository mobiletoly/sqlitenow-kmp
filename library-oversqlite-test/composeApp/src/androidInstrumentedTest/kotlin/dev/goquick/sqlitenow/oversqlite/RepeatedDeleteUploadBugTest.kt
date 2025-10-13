/*
 * Copyright 2025 Anatoliy Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.Assert.assertTrue
import org.junit.Before
import java.util.UUID

@RunWith(AndroidJUnit4::class)
class RepeatedDeleteUploadBugTest {

    @Before
    fun setUp() {
        if (skipAllOversqliteTest) {
            throw UnsupportedOperationException("(TEMPORARY setUp) Not implemented yet")
        }
    }

    @Test
    fun test_delete_operation_not_repeated_after_successful_upload() = runBlockingTest {
        println("\n🐛 REPRODUCING REPEATED DELETE UPLOAD BUG")
        println("Issue: DELETE operations keep getting uploaded repeatedly after successful sync")

        // Setup single device with verbose logging enabled
        val userId = "user-delete-${UUID.randomUUID().toString().substring(0, 8)}"
        val deviceId = "device-delete-test"

        val db = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))

        // Create table with BLOB primary key (like the real app where the bug occurs)
        // This matches the person table structure from the sample app
        db.execSQL("""
            CREATE TABLE users (
                id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),
                name TEXT NOT NULL,
                email TEXT NOT NULL
            ) WITHOUT ROWID
        """)

        val client = createSyncTestClient(
            db = db,
            userSub = userId,
            deviceId = deviceId,
            tables = listOf("users"),
            verboseLogs = true  // Enable verbose logging to see the issue
        )

        // Bootstrap the device
        println("\n📋 STEP 1: Bootstrap device")
        assertTrue(client.bootstrap(userId, deviceId).isSuccess)
        println("✅ Bootstrap completed successfully")

        // Insert and sync a record with BLOB primary key
        println("\n📋 STEP 2: Insert and sync a record")
        // Use randomblob(16) to generate a BLOB UUID like the real app
        db.execSQL("INSERT INTO users (name, email) VALUES ('Test User', 'test@example.com')")

        val initialUploadResult = client.uploadOnce()
        assertUploadSuccess(initialUploadResult, expectedApplied = 1)
        println("✅ Initial record uploaded successfully")

        // Verify no pending changes after initial sync
        val pendingAfterInsert = getPendingChangesCount(db)
        println("📊 Pending changes after INSERT sync: $pendingAfterInsert (should be 0)")
        assert(pendingAfterInsert == 0) { "Expected 0 pending changes after INSERT sync, got $pendingAfterInsert" }

        // Delete the record (delete the only user we inserted)
        println("\n📋 STEP 3: Delete the record")
        db.execSQL("DELETE FROM users WHERE name = 'Test User'")
        println("✅ Record deleted locally")

        // Verify there's 1 pending DELETE change
        val pendingAfterDelete = getPendingChangesCount(db)
        println("📊 Pending changes after DELETE: $pendingAfterDelete (should be 1)")
        assert(pendingAfterDelete == 1) { "Expected 1 pending change after DELETE, got $pendingAfterDelete" }

        // Get the DELETE change details
        val deleteChange = getPendingChangeDetails(db)
        println("🔍 DELETE change details: $deleteChange")
        assert(deleteChange?.contains("DELETE") == true) { "Expected DELETE operation in pending changes" }

        // Upload the DELETE operation (first time)
        println("\n📋 STEP 4: Upload DELETE operation (first time)")
        val deleteUploadResult1 = client.uploadOnce()
        assertUploadSuccess(deleteUploadResult1, expectedApplied = 1)
        println("✅ DELETE operation uploaded successfully (first time)")

        // Check pending changes after first DELETE upload - this is where the bug manifests
        val pendingAfterFirstUpload = getPendingChangesCount(db)
        println("📊 Pending changes after first DELETE upload: $pendingAfterFirstUpload")

        if (pendingAfterFirstUpload > 0) {
            val remainingChange = getPendingChangeDetails(db)
            println("🐛 BUG DETECTED: DELETE change still pending after successful upload!")
            println("   Remaining change: $remainingChange")

            // Try uploading again (second time) - this reproduces the exact bug from ADB logs
            println("\n📋 STEP 5: Upload again (second time) - reproducing repeated DELETE upload")
            val deleteUploadResult2 = client.uploadOnce()

            deleteUploadResult2.onSuccess { summary ->
                if (summary.total > 0) {
                    println("🐛 BUG REPRODUCED: DELETE operation uploaded again!")
                    println("   Upload result: total=${summary.total}, applied=${summary.applied}")

                    // This matches the exact behavior seen in ADB logs:
                    // - Same DELETE operation uploaded multiple times
                    // - Server responds with status=applied but newServerVersion=null (already deleted)

                    // FAIL THE TEST when bug is reproduced
                    throw AssertionError("🐛 REPEATED DELETE UPLOAD BUG DETECTED! DELETE operation was uploaded ${summary.total} times after already being successfully uploaded and acknowledged by server. This wastes network traffic and server storage.")
                } else {
                    throw AssertionError("Bug not reproduced - expected repeated DELETE upload but got total=0")
                }
            }.onFailure {
                throw AssertionError("Second upload failed: ${it.message}")
            }
        } else {
            println("✅ No bug detected: DELETE change was properly cleaned up after upload")
            println("   This means the bug has been fixed!")
        }

        // Final check of pending changes
        val finalPendingChanges = getPendingChangesCount(db)
        println("\nFinal pending changes count: $finalPendingChanges")
        
        if (finalPendingChanges > 0) {
            val finalChange = getPendingChangeDetails(db)
            println("FINAL BUG STATE: DELETE change still pending: $finalChange")
        }

        println("\nTEST COMPLETED")
    }

    private suspend fun getPendingChangesCount(db: SafeSQLiteConnection): Int {
        return db.prepare("SELECT COUNT(*) FROM _sync_pending").use { st ->
            st.step()
            st.getLong(0).toInt()
        }
    }

    private suspend fun getPendingChangeDetails(db: SafeSQLiteConnection): String? {
        return db.prepare("""
            SELECT table_name || ' ' || op || ' pk=' || substr(pk_uuid, 1, 8) || '...'
            FROM _sync_pending
            LIMIT 1
        """).use { st ->
            if (st.step()) st.getText(0) else null
        }
    }

    private suspend inline fun SafeSQLiteConnection.execSQL(sql: String, block: (SQLiteStatement) -> Unit = {}) {
        prepare(sql).use { st ->
            block(st)
            st.step()
        }
    }
}
