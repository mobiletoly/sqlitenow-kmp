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

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import java.util.UUID

/**
 * Integration test to reproduce the "record resurrection" bug in DELETE vs UPDATE conflicts.
 *
 * Bug Description:
 * When Device 1 deletes a record while Device 2 has pending updates for the same record,
 * the deleted record incorrectly "resurrects" (reappears) on Device 1 after synchronization.
 */
@RunWith(AndroidJUnit4::class)
class DeleteConflictResurrectionTest {

    @Before
    fun setUp() {
        if (skipAllOversqliteTest) {
            throw UnsupportedOperationException("(TEMPORARY setUp) Not implemented yet")
        }
    }

    /**
     * Creates a test client following the working pattern from existing tests
     */
    private suspend fun createTestClient(userId: String, deviceId: String): TestClient {
        val db = newInMemoryDb()

        createBusinessTables(db)

        val client = createSyncTestClient(
            db = db,
            userSub = userId,
            deviceId = deviceId,
            tables = listOf("users", "posts")
        )

        return TestClient(db, client, userId, deviceId)
    }



    /**
     * TestClient wrapper following the working pattern
     */
    data class TestClient(val db: SafeSQLiteConnection, val client: DefaultOversqliteClient, val userId: String, val deviceId: String) {
        suspend fun bootstrap() {
            println("Device $deviceId: Starting bootstrap...")
            val bootstrapResult = client.bootstrap(userId, deviceId)
            println("Device $deviceId: Bootstrap result: $bootstrapResult")
            assert(bootstrapResult.isSuccess) { "Bootstrap failed: ${bootstrapResult.exceptionOrNull()}" }

            val hydrateResult = client.hydrate(includeSelf = false, limit = 1000, windowed = true)
            println("Device $deviceId: Hydrate result: $hydrateResult")
            assert(hydrateResult.isSuccess) { "Hydrate failed: ${hydrateResult.exceptionOrNull()}" }
            println("Device $deviceId: Bootstrap complete")
        }

        suspend fun insertUser(id: String, name: String, email: String) {
            println("Device $deviceId: Inserting user $name...")
            db.prepare("INSERT INTO users (id, name, email) VALUES (?, ?, ?)").use { st ->
                st.bindText(1, id)
                st.bindText(2, name)
                st.bindText(3, email)
                st.step()
            }
            println("Device $deviceId: User inserted successfully")
            printSyncState("after INSERT")
        }

        suspend fun updateUser(id: String, name: String, email: String) {
            println("Device $deviceId: Updating user $name...")
            db.prepare("UPDATE users SET name = ?, email = ? WHERE id = ?").use { st ->
                st.bindText(1, name)
                st.bindText(2, email)
                st.bindText(3, id)
                st.step()
            }
            println("Device $deviceId: User updated successfully")
            printSyncState("after UPDATE")
        }

        suspend fun deleteUser(id: String) {
            println("Device $deviceId: Deleting user $id...")
            db.prepare("DELETE FROM users WHERE id = ?").use { st ->
                st.bindText(1, id)
                st.step()
            }
            println("Device $deviceId: User deleted successfully")
            printSyncState("after DELETE")
        }

        suspend fun userExists(id: String): Boolean {
            db.prepare("SELECT COUNT(*) FROM users WHERE id = ?").use { st ->
                st.bindText(1, id)
                return if (st.step()) st.getLong(0) > 0 else false
            }
        }

        suspend fun getUser(id: String): Triple<String, String, String>? {
            return db.prepare("SELECT id, name, email FROM users WHERE id = ?").use { st ->
                st.bindText(1, id)
                if (st.step()) {
                    Triple(st.getText(0), st.getText(1), st.getText(2))
                } else {
                    null
                }
            }
        }

        suspend fun printSyncState(context: String) {
            println("Device $deviceId sync state $context:")

            // Print pending changes
            db.prepare("SELECT table_name, pk_uuid, op, base_version, payload FROM _sync_pending ORDER BY change_id").use { st ->
                var pendingCount = 0
                while (st.step()) {
                    pendingCount++
                    val table = st.getText(0)
                    val pk = st.getText(1).take(8)
                    val op = st.getText(2)
                    val baseVersion = st.getLong(3)
                    val payload = st.getText(4).take(50)
                    println("  Pending[$pendingCount]: $table:$pk $op v$baseVersion payload=$payload")
                }
                if (pendingCount == 0) println("  No pending changes")
            }

            // Print row metadata
            db.prepare("SELECT table_name, pk_uuid, server_version, deleted FROM _sync_row_meta ORDER BY table_name, pk_uuid").use { st ->
                var metaCount = 0
                while (st.step()) {
                    metaCount++
                    val table = st.getText(0)
                    val pk = st.getText(1).take(8)
                    val serverVersion = st.getLong(2)
                    val deleted = st.getLong(3) == 1L
                    println("  Meta[$metaCount]: $table:$pk server_v$serverVersion deleted=$deleted")
                }
                if (metaCount == 0) println("  No row metadata")
            }

            // Print business data
            db.prepare("SELECT id, name, email FROM users ORDER BY name").use { st ->
                var userCount = 0
                while (st.step()) {
                    userCount++
                    val id = st.getText(0).take(8)
                    val name = st.getText(1)
                    val email = st.getText(2)
                    println("  User[$userCount]: $id $name $email")
                }
                if (userCount == 0) println("  No users")
            }
        }
    }

    /**
     * REPRODUCTION TEST: Record Resurrection Bug
     *
     * Exact Steps to Reproduce:
     * 1. Device 1: Add user A, then sync (user A exists on server)
     * 2. Device 2: Sync (user A now exists on both devices)
     * 3. Device 2: Update user A locally, then sync (server now has updated user A)
     * 4. Device 1: Delete user A locally, then sync (this should delete user A from server)
     * 5. BUG: On Device 1, user A incorrectly reappears/resurrects
     */
    @Test
    fun test_record_resurrection_bug_delete_vs_update_conflict() = runBlockingTest {
        println("\n🐛 === REPRODUCING RECORD RESURRECTION BUG ===")

        val userId = "test-user-${UUID.randomUUID().toString().substring(0, 8)}"
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        // Bootstrap both devices
        device1.bootstrap()
        device2.bootstrap()

        // Use a proper UUID format for the user ID
        val userAId = UUID.randomUUID().toString()

        println("\n📋 STEP 1: Device 1 adds user A, then sync")
        device1.insertUser(userAId, "Alice Original", "alice.original@example.com")
        assertTrue("User A should exist on Device 1", device1.userExists(userAId))

        println("\n🔄 Device 1 uploading INSERT...")
        val uploadResult = device1.client.uploadOnce()
        println("📤 Upload result: $uploadResult")
        assertUploadSuccess(uploadResult, expectedApplied = 1)
        device1.printSyncState("after upload")

        println("\n📋 STEP 2: Device 2 syncs (gets user A)")
        device2.printSyncState("before download")

        println("\n🔄 Device 2 downloading changes...")
        val downloadResult = device2.client.downloadOnce(limit = 1000, includeSelf = false)
        println("📥 Download result: $downloadResult")
        assertDownloadSuccess(downloadResult)
        device2.printSyncState("after download")

        val userExistsOnDevice2 = device2.userExists(userAId)
        println("\n🔍 User exists on Device 2: $userExistsOnDevice2")

        if (!userExistsOnDevice2) {
            println("\n❌ BASIC SYNC FAILURE DETECTED")
            println("Expected: User A should exist on Device 2 after download")
            println("Actual: User A does not exist on Device 2")
            println("\nDEBUG INFO:")
            println("- Device 1 uploaded INSERT successfully")
            println("- Device 2 downloaded successfully (no errors)")
            println("- But user is missing from Device 2")
            println("\nThis indicates a sync infrastructure issue, not the record resurrection bug")
        }

        assertTrue("User A should now exist on Device 2", userExistsOnDevice2)
        val userOnDevice2 = device2.getUser(userAId)
        assertEquals("User A should have original data on Device 2", "Alice Original", userOnDevice2?.second)

        println("✅ Basic sync working - proceeding to DELETE vs UPDATE conflict test")

        println("\n📋 STEP 3: Device 2 updates user A locally, then sync")
        device2.updateUser(userAId, "Alice Updated by Device2", "alice.updated@example.com")

        println("\n🔄 Device 2 uploading UPDATE...")
        val updateUploadResult = device2.client.uploadOnce()
        println("📤 Update upload result: $updateUploadResult")
        assertUploadSuccess(updateUploadResult, expectedApplied = 1)
        device2.printSyncState("after update upload")

        println("\n📋 STEP 4: Device 1 deletes user A locally, then sync")
        device1.deleteUser(userAId)
        assertFalse("User A should be deleted on Device 1", device1.userExists(userAId))

        println("\n🔄 Device 1 uploading DELETE (will conflict with Device 2's UPDATE)...")
        val deleteUploadResult = device1.client.uploadOnce()
        println("📤 Delete upload result: $deleteUploadResult")
        // This upload might have conflicts due to the DELETE vs UPDATE conflict, so use the lenient version
        assertUploadSuccessWithConflicts(deleteUploadResult)
        device1.printSyncState("after delete upload")

        println("\n📋 STEP 5: Check for record resurrection bug on Device 1")
        val userExistsAfterConflict = device1.userExists(userAId)
        println("🔍 User exists on Device 1 after DELETE conflict: $userExistsAfterConflict")

        if (userExistsAfterConflict) {
            val resurrectedUser = device1.getUser(userAId)
            println("\n🐛 RECORD RESURRECTION BUG DETECTED!")
            println("Expected: User A should remain deleted on Device 1")
            println("Actual: User A has been resurrected: $resurrectedUser")
            println("This demonstrates the DELETE vs UPDATE conflict resolution bug")
        } else {
            println("\n✅ No resurrection - DELETE properly maintained on Device 1")
        }

        assertFalse(
            "🐛 RECORD RESURRECTION BUG: User was deleted but reappeared after conflict!",
            userExistsAfterConflict
        )

        println("\n📋 STEP 6: Device 1 needs to upload the re-enqueued DELETE")
        device1.printSyncState("before second DELETE upload")

        println("\n🔄 Device 1 uploading re-enqueued DELETE to propagate to server...")
        val secondDeleteUploadResult = device1.client.uploadOnce()
        println("📤 Second DELETE upload result: $secondDeleteUploadResult")
        assertUploadSuccess(secondDeleteUploadResult, expectedApplied = 1)
        device1.printSyncState("after second DELETE upload")

        println("\n📋 STEP 7: Check Device 2 synchronization - should Device 2 get the DELETE?")
        val userExistsOnDevice2BeforeSync = device2.userExists(userAId)
        println("🔍 User exists on Device 2 BEFORE sync: $userExistsOnDevice2BeforeSync")

        if (userExistsOnDevice2BeforeSync) {
            val userOnDevice2 = device2.getUser(userAId)
            println("📄 Device 2 has user: $userOnDevice2")
        }

        println("\n🔄 Device 2 downloading changes to sync with Device 1's DELETE...")
        device2.printSyncState("before final sync")

        val finalDownloadResult = device2.client.downloadOnce(limit = 1000, includeSelf = false)
        println("📥 Final download result: $finalDownloadResult")
        assertDownloadSuccess(finalDownloadResult)
        device2.printSyncState("after final sync")

        println("\n📋 STEP 8: Verify both devices are synchronized")
        val userExistsOnDevice2AfterSync = device2.userExists(userAId)
        println("🔍 User exists on Device 2 AFTER sync: $userExistsOnDevice2AfterSync")

        if (userExistsOnDevice2AfterSync) {
            val userOnDevice2After = device2.getUser(userAId)
            println("\n🐛 SYNCHRONIZATION BUG DETECTED!")
            println("Expected: User A should be deleted on Device 2 after sync")
            println("Actual: Device 2 still has user: $userOnDevice2After")
            println("This means the DELETE from Device 1 didn't propagate to Device 2")
        } else {
            println("\n✅ Perfect sync - Device 2 also has user deleted")
        }

        // Verify both devices are in sync (both should have user deleted)
        val device1HasUser = device1.userExists(userAId)
        val device2HasUser = device2.userExists(userAId)

        println("\n📊 FINAL SYNC STATE:")
        println("   Device 1 has user: $device1HasUser")
        println("   Device 2 has user: $device2HasUser")
        println("   Devices synchronized: ${device1HasUser == device2HasUser}")

        assertFalse(
            "🐛 SYNCHRONIZATION BUG: Device 2 should also have user deleted after sync!",
            userExistsOnDevice2AfterSync
        )

        // Both devices should be in the same state (both should not have the user)
        assertEquals(
            "🐛 DEVICES OUT OF SYNC: Both devices should have the same user state after sync",
            device1HasUser,
            device2HasUser
        )
    }
}
