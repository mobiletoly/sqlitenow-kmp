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
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import java.util.UUID

@RunWith(AndroidJUnit4::class)
class SimpleConcurrentSyncTest {

    @Before
    fun setUp() {
        if (skipAllOversqliteTest) {
            throw UnsupportedOperationException("(TEMPORARY setUp) Not implemented yet")
        }
    }

    // Business schema is provided by TestHelpers.createBusinessTables(db)

    private suspend fun createTestDatabase(): SafeSQLiteConnection {
        val db = newInMemoryDb()
        createBusinessTables(db)
        return db
    }

    private suspend fun createSyncClient(
        db: SafeSQLiteConnection,
        userId: String,
        deviceId: String
    ): DefaultOversqliteClient {
        val client = createSyncTestClient(
            db = db,
            userSub = userId,
            deviceId = deviceId,
            tables = listOf("users", "posts")
        )

        val bootstrapResult = client.bootstrap(userId = userId, sourceId = deviceId)
        assertTrue("Bootstrap failed for $deviceId", bootstrapResult.isSuccess)

        return client
    }

    private suspend fun fullSync(client: DefaultOversqliteClient, deviceId: String) {
        println("[$deviceId] Starting full sync...")

        val uploadResult = client.uploadOnce()
        assertUploadSuccessWithConflicts(uploadResult)
        val uploadSummary = getUploadSummary(uploadResult)
        println("[$deviceId] Upload completed: $uploadSummary")

        val limit = 500
        var totalDownloaded = 0
        var more = true
        while (more) {
            val downloadResult = client.downloadOnce(limit = limit, includeSelf = false)
            assertDownloadSuccess(downloadResult)

            val (applied, _) = downloadResult.getOrNull() ?: (0 to 0L)
            totalDownloaded += applied
            more = applied == limit
            if (applied == 0) break
        }

        println("[$deviceId] Full sync completed. Downloaded: $totalDownloaded changes")
    }

    private suspend fun printAllMetadata(db: SafeSQLiteConnection, deviceId: String) {
        println("\n=== METADATA DEBUG FOR $deviceId ===")

        println("_sync_pending table:")
        db.prepare("SELECT table_name, pk_uuid, op, base_version FROM _sync_pending ORDER BY table_name, pk_uuid")
            .use { st ->
                while (st.step()) {
                    println(
                        "  PENDING: table=${st.getText(0)}, pk=${st.getText(1)}, op=${
                            st.getText(
                                2
                            )
                        }, base_v=${st.getLong(3)}"
                    )
                }
            }

        println("_sync_row_meta table:")
        db.prepare("SELECT table_name, pk_uuid, server_version, deleted FROM _sync_row_meta ORDER BY table_name, pk_uuid")
            .use { st ->
                while (st.step()) {
                    println(
                        "  META: table=${st.getText(0)}, pk=${st.getText(1)}, sv=${st.getLong(2)}, deleted=${
                            st.getLong(
                                3
                            ) == 1L
                        }"
                    )
                }
            }

        println("=== END METADATA DEBUG FOR $deviceId ===\n")
    }

    private suspend fun verifyDataConsistency(
        db1: SafeSQLiteConnection,
        db2: SafeSQLiteConnection,
        description: String
    ) {
        println("Verifying data consistency: $description")

        printAllMetadata(db1, "DB1")
        printAllMetadata(db2, "DB2")

        val users1Count = count(db1, "users", "1=1")
        val users2Count = count(db2, "users", "1=1")
        assertEquals("Users count mismatch in $description", users1Count, users2Count)

        val posts1Count = count(db1, "posts", "1=1")
        val posts2Count = count(db2, "posts", "1=1")
        assertEquals("Posts count mismatch in $description", posts1Count, posts2Count)

        if (users1Count > 0) {
            val user1Data = db1.prepare("SELECT id, name, email FROM users ORDER BY id").use { st ->
                val results = mutableListOf<Triple<String, String, String>>()
                while (st.step()) {
                    results.add(Triple(st.getText(0), st.getText(1), st.getText(2)))
                }
                results
            }

            val user2Data = db2.prepare("SELECT id, name, email FROM users ORDER BY id").use { st ->
                val results = mutableListOf<Triple<String, String, String>>()
                while (st.step()) {
                    results.add(Triple(st.getText(0), st.getText(1), st.getText(2)))
                }
                results
            }

            assertEquals("User data mismatch in $description", user1Data, user2Data)
        }

        println("✓ Data consistency verified: $description")
    }

    @Test
    fun debug_delete_race_condition_test() = runBlockingTest {
        println("=== Starting DELETE Race Condition Debug Test ===")

        // Setup two devices for DELETE debugging
        val userId = "user-delete-debug-${UUID.randomUUID().toString().substring(0, 8)}"
        val device1Id = "device-delete-debug-1"
        val device2Id = "device-delete-debug-2"

        val db1 = createTestDatabase()
        val db2 = createTestDatabase()

        val client1 = createSyncClient(db1, userId, device1Id)
        val client2 = createSyncClient(db2, userId, device2Id)

        // Initial hydration for both devices
        assertTrue(
            "Device 1 hydration failed",
            client1.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess
        )
        assertTrue(
            "Device 2 hydration failed",
            client2.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess
        )

        // Phase 1: Create test data (5 users + 5 posts)
        println("\n--- Phase 1: Create test data ---")
        val testUserIds = mutableListOf<String>()
        val testPostIds = mutableListOf<String>()

        repeat(5) { i ->
            val userId1 = UUID.randomUUID().toString()
            val userName = "TestUser-$i"
            val userEmail = "testuser$i@example.com"

            db1.execSQL("INSERT INTO users(id, name, email) VALUES('$userId1','$userName','$userEmail')")
            testUserIds.add(userId1)

            val postId1 = UUID.randomUUID().toString()
            val postTitle = "TestPost-$i"
            val postContent = "Test content $i"

            db1.execSQL("INSERT INTO posts(id, title, content, author_id) VALUES('$postId1','$postTitle','$postContent','$userId1')")
            testPostIds.add(postId1)
        }

        // Sync initial data
        println("Syncing initial test data...")
        repeat(3) { round ->
            println("Initial sync round ${round + 1}/3")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        verifyDataConsistency(db1, db2, "after initial test data creation")

        val initialUsers = count(db1, "users", "1=1")
        val initialPosts = count(db1, "posts", "1=1")
        println("Initial state: $initialUsers users, $initialPosts posts")

        // Phase 2: Sequential DELETE operations (no concurrency)
        println("\n--- Phase 2: Sequential DELETE operations ---")

        // Device 1: Delete 1 post
        val postToDelete = testPostIds[0]
        println("Device 1: Deleting post $postToDelete")
        db1.execSQL("DELETE FROM posts WHERE id='$postToDelete'")

        // Immediate sync after DELETE
        println("Syncing after Device 1 DELETE...")
        repeat(3) { round ->
            println("Post DELETE sync round ${round + 1}/3")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        verifyDataConsistency(db1, db2, "after sequential post DELETE")

        val postsAfterDelete = count(db1, "posts", "1=1")
        println("After post DELETE: $postsAfterDelete posts (expected: ${initialPosts - 1})")
        assertEquals("Post count should decrease by 1", initialPosts - 1, postsAfterDelete)

        // Device 2: Delete 1 user (with cascade)
        val userToDelete = testUserIds[1]
        println("Device 2: Deleting user $userToDelete (with cascade)")

        // First delete posts by this user
        db2.execSQL("DELETE FROM posts WHERE author_id='$userToDelete'")
        // Then delete the user
        db2.execSQL("DELETE FROM users WHERE id='$userToDelete'")

        // Immediate sync after user DELETE
        println("Syncing after Device 2 user DELETE...")

        repeat(3) { round ->
            println("User DELETE sync round ${round + 1}/3")
            fullSync(client2, device2Id)
            fullSync(client1, device1Id)
        }

        verifyDataConsistency(db1, db2, "after sequential user DELETE")

        val usersAfterDelete = count(db1, "users", "1=1")
        val postsAfterUserDelete = count(db1, "posts", "1=1")
        println("After user DELETE: $usersAfterDelete users (expected: ${initialUsers - 1}), $postsAfterUserDelete posts")
        assertEquals("User count should decrease by 1", initialUsers - 1, usersAfterDelete)

        // Phase 3: Concurrent DELETE operations (this is where race conditions occur)
        println("\n--- Phase 3: Concurrent DELETE operations ---")

        val remainingUsers = db1.prepare("SELECT id FROM users LIMIT 2").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        val remainingPosts = db1.prepare("SELECT id FROM posts LIMIT 2").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        println("Remaining users for concurrent DELETE: ${remainingUsers.size}")
        println("Remaining posts for concurrent DELETE: ${remainingPosts.size}")

        // Concurrent DELETE operations
        coroutineScope {
            val concurrentDeleteJobs = listOf(
                // Device 1: Delete 1 post
                async {
                    if (remainingPosts.isNotEmpty()) {
                        val postId = remainingPosts[0]
                        println("Device 1: Concurrently deleting post $postId")
                        db1.execSQL("DELETE FROM posts WHERE id='$postId'")

                        // Immediate sync
                        fullSync(client1, device1Id)
                    }
                },
                // Device 2: Delete 1 user (different from Device 1's target)
                async {
                    delay(50) // Small offset to create race condition
                    if (remainingUsers.size >= 2) {
                        val userId = remainingUsers[1] // Different user than Device 1 might target
                        println("Device 2: Concurrently deleting user $userId")

                        // Delete posts by this user first
                        db2.execSQL("DELETE FROM posts WHERE author_id='$userId'")
                        // Then delete the user
                        db2.execSQL("DELETE FROM users WHERE id='$userId'")

                        // Immediate sync
                        fullSync(client2, device2Id)
                    }
                }
            )

            concurrentDeleteJobs.awaitAll()
        }

        // Extensive sync after concurrent DELETEs
        println("\n--- Extensive sync after concurrent DELETEs ---")
        repeat(7) { round ->
            println("Concurrent DELETE sync round ${round + 1}/7")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
            // Double sync to ensure all DELETE operations are propagated
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        // Debug: Print detailed state before verification
        println("\n--- DEBUG: Detailed state before verification ---")
        printAllMetadata(db1, "DB1 after concurrent DELETEs")
        printAllMetadata(db2, "DB2 after concurrent DELETEs")

        val finalUsers1 = count(db1, "users", "1=1")
        val finalPosts1 = count(db1, "posts", "1=1")
        val finalUsers2 = count(db2, "users", "1=1")
        val finalPosts2 = count(db2, "posts", "1=1")

        println("Final counts - DB1: $finalUsers1 users, $finalPosts1 posts")
        println("Final counts - DB2: $finalUsers2 users, $finalPosts2 posts")

        // This is where the race condition manifests
        try {
            verifyDataConsistency(db1, db2, "after concurrent DELETE operations")
            println("✅ Concurrent DELETE operations completed successfully")
        } catch (e: AssertionError) {
            println("❌ RACE CONDITION DETECTED: ${e.message}")

            // Additional debugging
            println("\n--- RACE CONDITION ANALYSIS ---")

            // Check pending operations
            val pending1 =
                db1.prepare("SELECT table_name, pk_uuid, op FROM _sync_pending").use { st ->
                    val pending = mutableListOf<String>()
                    while (st.step()) {
                        pending.add("${st.getText(0)}:${st.getText(1).take(8)}:${st.getText(2)}")
                    }
                    pending
                }

            val pending2 =
                db2.prepare("SELECT table_name, pk_uuid, op FROM _sync_pending").use { st ->
                    val pending = mutableListOf<String>()
                    while (st.step()) {
                        pending.add("${st.getText(0)}:${st.getText(1).take(8)}:${st.getText(2)}")
                    }
                    pending
                }

            println("DB1 pending operations: $pending1")
            println("DB2 pending operations: $pending2")

            // Re-throw to fail the test and show the race condition
            throw e
        }

        // Cleanup
        client1.close()
        client2.close()
    }

    @Test
    fun insert_update_concurrent_sync_test() = runBlockingTest {
        println("=== Starting INSERT/UPDATE Concurrent Sync Test ===")

        // Setup two devices for INSERT/UPDATE concurrency testing (no DELETE operations)
        val userId = "user-insert-update-${UUID.randomUUID().toString().substring(0, 8)}"
        val device1Id = "device-insert-update-1"
        val device2Id = "device-insert-update-2"

        val db1 = createTestDatabase()
        val db2 = createTestDatabase()

        val client1 = createSyncClient(db1, userId, device1Id)
        val client2 = createSyncClient(db2, userId, device2Id)

        // Initial hydration for both devices
        assertTrue(
            "Device 1 hydration failed",
            client1.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess
        )
        assertTrue(
            "Device 2 hydration failed",
            client2.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess
        )

        // Phase 1: Large INSERT Operations (25 users + 40 posts per device)
        println("\n--- Phase 1: Large INSERT Operations ---")

        coroutineScope {
            val insertJobs = listOf(
                // Device 1: Insert 25 users + posts
                async {
                    repeat(25) { i ->
                        val userId1 = UUID.randomUUID().toString()
                        val userName = "User1-$i"
                        val userEmail = "user1-$i@device1.com"

                        db1.execSQL("INSERT INTO users(id, name, email) VALUES('$userId1','$userName','$userEmail')")

                        // Create 1-2 posts per user
                        val postsPerUser = if (i % 3 == 0) 2 else 1
                        repeat(postsPerUser) { j ->
                            val postId1 = UUID.randomUUID().toString()
                            val postTitle = "Post1-$i-$j"
                            val postContent = "Content from device 1 - user $i, post $j"

                            db1.execSQL("INSERT INTO posts(id, title, content, author_id) VALUES('$postId1','$postTitle','$postContent','$userId1')")
                        }

                        if (i % 5 == 0) {
                            println("[${device1Id}] INSERT progress: $i/25 users")
                            fullSync(client1, device1Id)
                        }
                        delay(15)
                    }
                    println("[${device1Id}] Completed INSERT operations")
                },
                // Device 2: Insert 25 users + posts
                async {
                    delay(20) // Slight offset for realistic concurrency
                    repeat(25) { i ->
                        val userId2 = UUID.randomUUID().toString()
                        val userName = "User2-$i"
                        val userEmail = "user2-$i@device2.com"

                        db2.execSQL("INSERT INTO users(id, name, email) VALUES('$userId2','$userName','$userEmail')")

                        // Create 1-2 posts per user
                        val postsPerUser = if (i % 4 == 0) 2 else 1
                        repeat(postsPerUser) { j ->
                            val postId2 = UUID.randomUUID().toString()
                            val postTitle = "Post2-$i-$j"
                            val postContent = "Content from device 2 - user $i, post $j"

                            db2.execSQL("INSERT INTO posts(id, title, content, author_id) VALUES('$postId2','$postTitle','$postContent','$userId2')")
                        }

                        if (i % 6 == 0) {
                            println("[${device2Id}] INSERT progress: $i/25 users")
                            fullSync(client2, device2Id)
                        }
                        delay(18)
                    }
                    println("[${device2Id}] Completed INSERT operations")
                }
            )

            insertJobs.awaitAll()
        }

        // Comprehensive sync after inserts using proven pattern
        println("\n--- Syncing after INSERTs ---")
        repeat(3) { round ->
            println("INSERT sync round ${round + 1}/3")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        verifyDataConsistency(db1, db2, "after large INSERT operations")

        val usersAfterInserts = count(db1, "users", "1=1")
        val postsAfterInserts = count(db1, "posts", "1=1")
        println("After INSERTs: Users=$usersAfterInserts, Posts=$postsAfterInserts")

        // Verify reasonable counts (should be around 50 users, 62-75 posts)
        assertTrue("Should have at least 48 users", usersAfterInserts >= 48)
        assertTrue("Should have at least 60 posts", postsAfterInserts >= 60)
        assertTrue("Should have at most 52 users", usersAfterInserts <= 52)
        assertTrue("Should have at most 80 posts", postsAfterInserts <= 80)

        println("\u2713 Phase 1 completed: Large INSERT operations verified")

        // Phase 2: Large UPDATE Operations with Many Conflicts
        println("\n--- Phase 2: Large UPDATE Operations with Many Conflicts ---")

        // Get existing records for updates
        val existingUserIds = db1.prepare("SELECT id FROM users LIMIT 30").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        val existingPostIds = db1.prepare("SELECT id FROM posts LIMIT 40").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }



        coroutineScope {
            val updateJobs = listOf(
                // Device 1: Update 20 users + 25 posts with conflicts
                async {
                    existingUserIds.take(20).forEachIndexed { i, userId ->
                        val timestamp = System.currentTimeMillis()
                        val newName = "UpdatedUser1-$i-$timestamp"
                        val newEmail = "updated1-$i-$timestamp@device1.com"

                        db1.execSQL("UPDATE users SET name='$newName', email='$newEmail' WHERE id='$userId'")

                        if (i % 4 == 0) {
                            println("[${device1Id}] UPDATE users progress: $i/20")
                            fullSync(client1, device1Id)
                        }
                        delay(25)
                    }

                    existingPostIds.take(25).forEachIndexed { i, postId ->
                        val timestamp = System.currentTimeMillis()
                        val newTitle = "UpdatedPost1-$i-$timestamp"
                        val newContent = "Updated content from device 1 - $i - $timestamp"

                        db1.execSQL("UPDATE posts SET title='$newTitle', content='$newContent' WHERE id='$postId'")

                        if (i % 5 == 0) {
                            println("[${device1Id}] UPDATE posts progress: $i/25")
                            fullSync(client1, device1Id)
                        }
                        delay(30)
                    }
                    println("[${device1Id}] Completed UPDATE operations")
                },
                // Device 2: Update overlapping records to create many conflicts
                async {
                    delay(40) // Offset to create realistic conflicts

                    // Update users with overlapping IDs (10-30 range overlaps with device 1's 0-20)
                    existingUserIds.drop(10).take(20).forEachIndexed { i, userId ->
                        val timestamp = System.currentTimeMillis()
                        val newName = "UpdatedUser2-$i-$timestamp"
                        val newEmail = "updated2-$i-$timestamp@device2.com"

                        db2.execSQL("UPDATE users SET name='$newName', email='$newEmail' WHERE id='$userId'")

                        if (i % 4 == 0) {
                            println("[${device2Id}] UPDATE users progress: $i/20")
                            fullSync(client2, device2Id)
                        }
                        delay(28)
                    }

                    // Update posts with overlapping IDs (12-37 range overlaps with device 1's 0-25)
                    existingPostIds.drop(12).take(25).forEachIndexed { i, postId ->
                        val timestamp = System.currentTimeMillis()
                        val newTitle = "UpdatedPost2-$i-$timestamp"
                        val newContent = "Updated content from device 2 - $i - $timestamp"

                        db2.execSQL("UPDATE posts SET title='$newTitle', content='$newContent' WHERE id='$postId'")

                        if (i % 5 == 0) {
                            println("[${device2Id}] UPDATE posts progress: $i/25")
                            fullSync(client2, device2Id)
                        }
                        delay(35)
                    }
                    println("[${device2Id}] Completed UPDATE operations")
                }
            )

            updateJobs.awaitAll()
        }

        // Comprehensive sync after updates with conflict resolution using proven pattern
        println("\n--- Syncing after UPDATEs (with extensive conflict resolution) ---")
        repeat(7) { round ->
            println("UPDATE sync round ${round + 1}/7 (resolving many conflicts)")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
            // Additional sync to ensure conflict resolution uploads are processed
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        verifyDataConsistency(db1, db2, "after large UPDATE operations with many conflicts")

        println("\u2713 Phase 2 completed: Large UPDATE operations with many conflicts resolved")

        // Phase 3: Additional INSERT Operations (to test mixed operations)
        println("\n--- Phase 3: Additional INSERT Operations ---")

        coroutineScope {
            val additionalInsertJobs = listOf(
                // Device 1: Insert 10 more users + posts
                async {
                    repeat(10) { i ->
                        val userId1 = UUID.randomUUID().toString()
                        val userName = "ExtraUser1-$i"
                        val userEmail = "extra1-$i@device1.com"

                        db1.execSQL("INSERT INTO users(id, name, email) VALUES('$userId1','$userName','$userEmail')")

                        // Create 1 post per user
                        val postId1 = UUID.randomUUID().toString()
                        val postTitle = "ExtraPost1-$i"
                        val postContent = "Extra content from device 1 - user $i"

                        db1.execSQL("INSERT INTO posts(id, title, content, author_id) VALUES('$postId1','$postTitle','$postContent','$userId1')")

                        if (i % 3 == 0) {
                            println("[${device1Id}] Extra INSERT progress: $i/10 users")
                            fullSync(client1, device1Id)
                        }
                        delay(20)
                    }
                    println("[${device1Id}] Completed additional INSERT operations")
                },
                // Device 2: Insert 10 more users + posts
                async {
                    delay(25)
                    repeat(10) { i ->
                        val userId2 = UUID.randomUUID().toString()
                        val userName = "ExtraUser2-$i"
                        val userEmail = "extra2-$i@device2.com"

                        db2.execSQL("INSERT INTO users(id, name, email) VALUES('$userId2','$userName','$userEmail')")

                        // Create 1 post per user
                        val postId2 = UUID.randomUUID().toString()
                        val postTitle = "ExtraPost2-$i"
                        val postContent = "Extra content from device 2 - user $i"

                        db2.execSQL("INSERT INTO posts(id, title, content, author_id) VALUES('$postId2','$postTitle','$postContent','$userId2')")

                        if (i % 4 == 0) {
                            println("[${device2Id}] Extra INSERT progress: $i/10 users")
                            fullSync(client2, device2Id)
                        }
                        delay(22)
                    }
                    println("[${device2Id}] Completed additional INSERT operations")
                }
            )

            additionalInsertJobs.awaitAll()
        }


        // Final sync after additional inserts
        println("\n--- Syncing after additional INSERTs ---")
        repeat(3) { round ->
            println("Additional INSERT sync round ${round + 1}/3")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        verifyDataConsistency(db1, db2, "after additional INSERT operations")

        println("\u2713 Phase 3 completed: Additional INSERT operations")

        // Final verification using proven pattern
        println("\n--- Final Verification ---")
        repeat(5) { round ->
            println("Final verification sync round ${round + 1}/5")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
            // Double sync each round to ensure all conflict resolutions are processed
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        // Comprehensive data consistency verification
        verifyDataConsistency(db1, db2, "final INSERT/UPDATE verification")

        val finalUsersCount = count(db1, "users", "1=1")
        val finalPostsCount = count(db1, "posts", "1=1")

        println("\n=== INSERT/UPDATE CONCURRENT SYNC TEST VERIFICATION ===")
        println("Final users: $finalUsersCount")
        println("Final posts: $finalPostsCount")

        // Verify we have expected counts (should be around 70 users, 82-90 posts)
        assertTrue("Should have at least 68 users", finalUsersCount >= 68)
        assertTrue("Should have at least 80 posts", finalPostsCount >= 80)
        assertTrue("Should have at most 72 users", finalUsersCount <= 72)
        assertTrue("Should have at most 95 posts", finalPostsCount <= 95)

        // Verify foreign key integrity
        val orphanedPosts1 = count(db1, "posts", "author_id NOT IN (SELECT id FROM users)")
        val orphanedPosts2 = count(db2, "posts", "author_id NOT IN (SELECT id FROM users)")
        assertEquals("No orphaned posts should exist in db1", 0, orphanedPosts1)
        assertEquals("No orphaned posts should exist in db2", 0, orphanedPosts2)

        // Verify data content consistency for a sample of records
        val sampleUserIds = db1.prepare("SELECT id FROM users LIMIT 10").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        sampleUserIds.forEach { userId ->
            val userData1 = db1.prepare("SELECT name, email FROM users WHERE id=?").use { st ->
                st.bindText(1, userId)
                if (st.step()) Pair(st.getText(0), st.getText(1)) else null
            }
            val userData2 = db2.prepare("SELECT name, email FROM users WHERE id=?").use { st ->
                st.bindText(1, userId)
                if (st.step()) Pair(st.getText(0), st.getText(1)) else null
            }
            assertEquals(
                "User data should be identical across devices for $userId",
                userData1,
                userData2
            )
        }

        println("\n\uD83C\uDF89 INSERT/UPDATE CONCURRENT SYNC TEST PASSED! \uD83C\uDF89")
        println("\u2705 Final state: $finalUsersCount users, $finalPostsCount posts")
        println("\u2705 Data consistency maintained across both devices")
        println("\u2705 Large concurrent INSERT/UPDATE operations completed successfully")
        println("\u2705 Many UPDATE conflicts resolved properly")
        println("\u2705 Foreign key integrity maintained across all operations")
        println("\u2705 Mixed INSERT/UPDATE operations handled correctly")

        // Cleanup
        client1.close()
        client2.close()
    }


    @Test
    fun intensive_delete_race_condition_test() = runBlockingTest {
        println("=== Starting Intensive DELETE Race Condition Test ===")

        // Setup two devices for intensive DELETE testing
        val userId = "user-intensive-delete-${UUID.randomUUID().toString().substring(0, 8)}"
        val device1Id = "device-intensive-delete-1"
        val device2Id = "device-intensive-delete-2"

        val db1 = createTestDatabase()
        val db2 = createTestDatabase()

        val client1 = createSyncClient(db1, userId, device1Id)
        val client2 = createSyncClient(db2, userId, device2Id)

        // Initial hydration for both devices
        assertTrue(
            "Device 1 hydration failed",
            client1.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess
        )
        assertTrue(
            "Device 2 hydration failed",
            client2.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess
        )

        // Phase 1: Create substantial test data (20 users + 30 posts)
        println("\n--- Phase 1: Create substantial test data ---")
        val testUserIds = mutableListOf<String>()
        val testPostIds = mutableListOf<String>()

        repeat(20) { i ->
            val userId1 = UUID.randomUUID().toString()
            val userName = "TestUser-$i"
            val userEmail = "testuser$i@example.com"

            db1.execSQL("INSERT INTO users(id, name, email) VALUES('$userId1','$userName','$userEmail')")
            testUserIds.add(userId1)

            // Create 1-2 posts per user
            val postsPerUser = if (i % 3 == 0) 2 else 1
            repeat(postsPerUser) { j ->
                val postId1 = UUID.randomUUID().toString()
                val postTitle = "TestPost-$i-$j"
                val postContent = "Test content $i-$j"

                db1.execSQL("INSERT INTO posts(id, title, content, author_id) VALUES('$postId1','$postTitle','$postContent','$userId1')")
                testPostIds.add(postId1)
            }
        }

        // Sync initial data
        println("Syncing initial test data...")
        repeat(3) { round ->
            println("Initial sync round ${round + 1}/3")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        verifyDataConsistency(db1, db2, "after initial test data creation")

        val initialUsers = count(db1, "users", "1=1")
        val initialPosts = count(db1, "posts", "1=1")
        println("Initial state: $initialUsers users, $initialPosts posts")

        // Phase 2: High-volume concurrent DELETE operations (this should reproduce the race condition)
        println("\n--- Phase 2: High-volume concurrent DELETE operations ---")

        // Get records for deletion
        val usersForDeletion = db1.prepare("SELECT id FROM users LIMIT 10").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        val postsForDeletion = db1.prepare("SELECT id FROM posts LIMIT 15").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        println("Users available for deletion: ${usersForDeletion.size}")
        println("Posts available for deletion: ${postsForDeletion.size}")

        // Concurrent DELETE operations with realistic timing
        coroutineScope {
            val intensiveDeleteJobs = listOf(
                // Device 1: Delete 5 posts + 3 users
                async {
                    // Delete posts first
                    postsForDeletion.take(5).forEachIndexed { i, postId ->
                        println("Device 1: Deleting post $i/5")
                        db1.execSQL("DELETE FROM posts WHERE id='$postId'")

                        if (i % 2 == 0) {
                            fullSync(client1, device1Id)
                        }
                        delay(30)
                    }

                    // Then delete users (with cascade)
                    usersForDeletion.take(3).forEachIndexed { i, userId ->
                        println("Device 1: Deleting user $i/3 (with cascade)")
                        // Delete posts by this user first
                        db1.execSQL("DELETE FROM posts WHERE author_id='$userId'")
                        // Then delete the user
                        db1.execSQL("DELETE FROM users WHERE id='$userId'")

                        if (i % 2 == 0) {
                            fullSync(client1, device1Id)
                        }
                        delay(50)
                    }
                    println("Device 1: Completed intensive DELETE operations")
                },
                // Device 2: Delete different posts + users (overlapping timing)
                async {
                    delay(40) // Offset to create race conditions

                    // Delete different posts
                    postsForDeletion.drop(5).take(5).forEachIndexed { i, postId ->
                        println("Device 2: Deleting post $i/5")
                        db2.execSQL("DELETE FROM posts WHERE id='$postId'")

                        if (i % 2 == 0) {
                            fullSync(client2, device2Id)
                        }
                        delay(35)
                    }

                    // Delete different users (with cascade)
                    usersForDeletion.drop(3).take(3).forEachIndexed { i, userId ->
                        println("Device 2: Deleting user $i/3 (with cascade)")
                        // Delete posts by this user first
                        db2.execSQL("DELETE FROM posts WHERE author_id='$userId'")
                        // Then delete the user
                        db2.execSQL("DELETE FROM users WHERE id='$userId'")

                        if (i % 2 == 0) {
                            fullSync(client2, device2Id)
                        }
                        delay(45)
                    }
                    println("Device 2: Completed intensive DELETE operations")
                }
            )

            intensiveDeleteJobs.awaitAll()
        }

        // Extensive sync after intensive DELETEs (using the same pattern as UPDATE conflicts)
        println("\n--- Extensive sync after intensive DELETEs ---")
        repeat(7) { round ->
            println("Intensive DELETE sync round ${round + 1}/7")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
            // Double sync to ensure all DELETE operations are propagated
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        // Debug: Print detailed state before verification
        println("\n--- DEBUG: Detailed state before verification ---")
        val finalUsers1 = count(db1, "users", "1=1")
        val finalPosts1 = count(db1, "posts", "1=1")
        val finalUsers2 = count(db2, "users", "1=1")
        val finalPosts2 = count(db2, "posts", "1=1")

        println("Final counts - DB1: $finalUsers1 users, $finalPosts1 posts")
        println("Final counts - DB2: $finalUsers2 users, $finalPosts2 posts")

        // Check for count mismatches (this is where the race condition manifests)
        if (finalUsers1 != finalUsers2 || finalPosts1 != finalPosts2) {
            println("❌ RACE CONDITION DETECTED!")
            println("User count mismatch: DB1=$finalUsers1, DB2=$finalUsers2")
            println("Post count mismatch: DB1=$finalPosts1, DB2=$finalPosts2")

            // Additional debugging
            printAllMetadata(db1, "DB1 after intensive DELETEs")
            printAllMetadata(db2, "DB2 after intensive DELETEs")

            // Check pending operations
            val pending1 =
                db1.prepare("SELECT table_name, pk_uuid, op FROM _sync_pending").use { st ->
                    val pending = mutableListOf<String>()
                    while (st.step()) {
                        pending.add("${st.getText(0)}:${st.getText(1).take(8)}:${st.getText(2)}")
                    }
                    pending
                }

            val pending2 =
                db2.prepare("SELECT table_name, pk_uuid, op FROM _sync_pending").use { st ->
                    val pending = mutableListOf<String>()
                    while (st.step()) {
                        pending.add("${st.getText(0)}:${st.getText(1).take(8)}:${st.getText(2)}")
                    }
                    pending
                }

            println("DB1 pending operations: $pending1")
            println("DB2 pending operations: $pending2")
        }

        // This should trigger the race condition
        try {
            verifyDataConsistency(db1, db2, "after intensive DELETE operations")
            println("✅ Intensive DELETE operations completed successfully")
        } catch (e: AssertionError) {
            println("❌ INTENSIVE DELETE RACE CONDITION CONFIRMED: ${e.message}")
            throw e
        }

        // Cleanup
        client1.close()
        client2.close()
    }


}
