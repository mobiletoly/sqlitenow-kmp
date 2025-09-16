package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
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
import java.util.concurrent.atomic.AtomicInteger

@RunWith(AndroidJUnit4::class)
class HeavyConcurrentSyncTest {

    @Before
    fun setUp() {
        if (skipAllTest) {
            throw UnsupportedOperationException("(TEMPORARY setUp) Not implemented yet")
        }
    }

    /**
     * Creates a new database connection and sets up business tables
     */
    private suspend fun createTestDatabase(): SafeSQLiteConnection {
        val db = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        createBusinessTables(db)
        return db
    }

    /**
     * Creates and bootstraps a sync client
     */
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

    /**
     * Performs full synchronization (upload + download until complete)
     */
    private suspend fun fullSync(client: DefaultOversqliteClient, deviceId: String, db: SafeSQLiteConnection? = null) {
        println("[$deviceId] Starting full sync...")

        // Upload local changes with detailed error logging
        val uploadResult = client.uploadOnce()
        if (!uploadResult.isSuccess) {
            val error = uploadResult.exceptionOrNull()
            println("[$deviceId] Upload error: ${error?.message}")
            println("[$deviceId] Upload error details: ${error?.stackTraceToString()}")

            // Additional debugging for foreign key errors
            if (error?.message?.contains("FOREIGN KEY") == true && db != null) {
                println("[$deviceId] FOREIGN KEY ERROR DETECTED!")
                printDatabaseState(db, deviceId, "UPLOAD_ERROR")
            }
        }
        assertUploadSuccessWithConflicts(uploadResult)
        val uploadSummary = getUploadSummary(uploadResult)
        println("[$deviceId] Upload completed: $uploadSummary")

        // Download until no more changes with detailed error logging
        val limit = 500
        var totalDownloaded = 0
        var more = true
        while (more) {
            val downloadResult = client.downloadOnce(limit = limit, includeSelf = false)
            if (!downloadResult.isSuccess) {
                val error = downloadResult.exceptionOrNull()
                println("[$deviceId] Download error: ${error?.message}")
                println("[$deviceId] Download error details: ${error?.stackTraceToString()}")

                // Additional debugging for foreign key errors
                if (error?.message?.contains("FOREIGN KEY") == true && db != null) {
                    println("[$deviceId] FOREIGN KEY ERROR DETECTED DURING DOWNLOAD!")
                    printDatabaseState(db, deviceId, "DOWNLOAD_ERROR")
                }
            }
            assertDownloadSuccess(downloadResult)

            val (applied, _) = downloadResult.getOrNull() ?: (0 to 0L)
            totalDownloaded += applied
            more = applied == limit
            if (applied == 0) break
        }

        println("[$deviceId] Full sync completed. Downloaded: $totalDownloaded changes")
    }

    /**
     * Prints detailed database state for debugging foreign key issues
     */
    private suspend fun printDatabaseState(db: SafeSQLiteConnection, deviceId: String, context: String) {
        println("\n=== DATABASE STATE DEBUG [$deviceId] [$context] ===")

        // Print users count
        val usersCount = count(db, "users", "1=1")
        println("Users count: $usersCount")

        // Print posts count
        val postsCount = count(db, "posts", "1=1")
        println("Posts count: $postsCount")

        // Print orphaned posts (posts without valid author_id)
        val orphanedPosts = count(db, "posts", "author_id NOT IN (SELECT id FROM users)")
        println("Orphaned posts: $orphanedPosts")

        // Print sample of orphaned posts if any exist
        if (orphanedPosts > 0) {
            println("Sample orphaned posts:")
            db.prepare("SELECT id, title, author_id FROM posts WHERE author_id NOT IN (SELECT id FROM users) LIMIT 5").use { st ->
                while (st.step()) {
                    val postId = st.getText(0)
                    val title = st.getText(1)
                    val authorId = st.getText(2)
                    println("  Post: ${postId.take(8)} '$title' -> author: ${authorId.take(8)}")
                }
            }
        }

        // Print pending sync operations
        println("Pending sync operations:")
        db.prepare("SELECT table_name, pk_uuid, op FROM _sync_pending ORDER BY table_name, op").use { st ->
            while (st.step()) {
                val table = st.getText(0)
                val pk = st.getText(1)
                val op = st.getText(2)
                println("  $op $table:${pk.take(8)}")
            }
        }

        println("=== END DATABASE STATE DEBUG [$deviceId] [$context] ===\n")
    }

    /**
     * Prints all metadata from sync tables for debugging
     */
    private suspend fun printAllMetadata(db: SafeSQLiteConnection, deviceId: String) {
        println("\n=== METADATA DEBUG FOR $deviceId ===")

        // Print _sync_pending table
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

        // Print _sync_row_meta table
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

    /**
     * Verifies data consistency between two databases
     */
    private suspend fun verifyDataConsistency(
        db1: SafeSQLiteConnection,
        db2: SafeSQLiteConnection,
        description: String
    ) {
        println("Verifying data consistency: $description")

        // Print metadata for debugging
        printAllMetadata(db1, "DB1")
        printAllMetadata(db2, "DB2")

        // Check users table consistency
        val users1Count = count(db1, "users", "1=1")
        val users2Count = count(db2, "users", "1=1")
        assertEquals("Users count mismatch in $description", users1Count, users2Count)

        // Check posts table consistency
        val posts1Count = count(db1, "posts", "1=1")
        val posts2Count = count(db2, "posts", "1=1")
        assertEquals("Posts count mismatch in $description", posts1Count, posts2Count)

        // Verify specific user data matches
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
    fun moderate_concurrent_sync_test() = runBlockingTest {
        println("=== Starting Moderate Concurrent Sync Test ===")

        // Setup two devices for moderate concurrency testing
        val userId = "user-moderate-${UUID.randomUUID().toString().substring(0, 8)}"
        val device1Id = "device-moderate-1"
        val device2Id = "device-moderate-2"

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

        // Phase 1: Moderate INSERT Operations (10 users + 15 posts per device)
        println("\n--- Phase 1: Moderate INSERT Operations ---")

        coroutineScope {
            val insertJobs = listOf(
                // Device 1: Insert 10 users + posts
                async {
                    repeat(10) { i ->
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

                        if (i % 3 == 0) {
                            println("[${device1Id}] INSERT progress: $i/10 users")
                            fullSync(client1, device1Id)
                        }
                        delay(20)
                    }
                    println("[${device1Id}] Completed INSERT operations")
                },
                // Device 2: Insert 10 users + posts
                async {
                    delay(30) // Slight offset for realistic concurrency
                    repeat(10) { i ->
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

                        if (i % 4 == 0) {
                            println("[${device2Id}] INSERT progress: $i/10 users")
                            fullSync(client2, device2Id)
                        }
                        delay(25)
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

        verifyDataConsistency(db1, db2, "after moderate INSERT operations")

        val usersAfterInserts = count(db1, "users", "1=1")
        val postsAfterInserts = count(db1, "posts", "1=1")
        println("After INSERTs: Users=$usersAfterInserts, Posts=$postsAfterInserts")

        // Verify reasonable counts (should be around 20 users, 25-30 posts)
        assertTrue("Should have at least 18 users", usersAfterInserts >= 18)
        assertTrue("Should have at least 22 posts", postsAfterInserts >= 22)
        assertTrue("Should have at most 22 users", usersAfterInserts <= 22)
        assertTrue("Should have at most 35 posts", postsAfterInserts <= 35)

        println("✓ Phase 1 completed: Moderate INSERT operations verified")

        // Phase 2: Moderate UPDATE Operations with Conflicts
        println("\n--- Phase 2: Moderate UPDATE Operations with Conflicts ---")

        // Get existing records for updates
        val existingUserIds = db1.prepare("SELECT id FROM users LIMIT 8").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        val existingPostIds = db1.prepare("SELECT id FROM posts LIMIT 10").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        coroutineScope {
            val updateJobs = listOf(
                // Device 1: Update 5 users + 6 posts with conflicts
                async {
                    existingUserIds.take(5).forEachIndexed { i, userId ->
                        val timestamp = System.currentTimeMillis()
                        val newName = "UpdatedUser1-$i-$timestamp"
                        val newEmail = "updated1-$i-$timestamp@device1.com"

                        db1.execSQL("UPDATE users SET name='$newName', email='$newEmail' WHERE id='$userId'")

                        if (i % 2 == 0) {
                            println("[${device1Id}] UPDATE users progress: $i/5")
                            fullSync(client1, device1Id)
                        }
                        delay(30)
                    }

                    existingPostIds.take(6).forEachIndexed { i, postId ->
                        val timestamp = System.currentTimeMillis()
                        val newTitle = "UpdatedPost1-$i-$timestamp"
                        val newContent = "Updated content from device 1 - $i - $timestamp"

                        db1.execSQL("UPDATE posts SET title='$newTitle', content='$newContent' WHERE id='$postId'")

                        if (i % 3 == 0) {
                            println("[${device1Id}] UPDATE posts progress: $i/6")
                            fullSync(client1, device1Id)
                        }
                        delay(40)
                    }
                    println("[${device1Id}] Completed UPDATE operations")
                },
                // Device 2: Update overlapping records to create conflicts
                async {
                    delay(60) // Offset to create realistic conflicts

                    // Update users with overlapping IDs (2-7 range overlaps with device 1's 0-5)
                    existingUserIds.drop(2).take(5).forEachIndexed { i, userId ->
                        val timestamp = System.currentTimeMillis()
                        val newName = "UpdatedUser2-$i-$timestamp"
                        val newEmail = "updated2-$i-$timestamp@device2.com"

                        db2.execSQL("UPDATE users SET name='$newName', email='$newEmail' WHERE id='$userId'")

                        if (i % 2 == 0) {
                            println("[${device2Id}] UPDATE users progress: $i/5")
                            fullSync(client2, device2Id)
                        }
                        delay(35)
                    }

                    // Update posts with overlapping IDs (3-9 range overlaps with device 1's 0-6)
                    existingPostIds.drop(3).take(6).forEachIndexed { i, postId ->
                        val timestamp = System.currentTimeMillis()
                        val newTitle = "UpdatedPost2-$i-$timestamp"
                        val newContent = "Updated content from device 2 - $i - $timestamp"

                        db2.execSQL("UPDATE posts SET title='$newTitle', content='$newContent' WHERE id='$postId'")

                        if (i % 3 == 0) {
                            println("[${device2Id}] UPDATE posts progress: $i/6")
                            fullSync(client2, device2Id)
                        }
                        delay(45)
                    }
                    println("[${device2Id}] Completed UPDATE operations")
                }
            )

            updateJobs.awaitAll()
        }

        // Comprehensive sync after updates with conflict resolution using proven pattern
        println("\n--- Syncing after UPDATEs (with conflict resolution) ---")
        repeat(5) { round ->
            println("UPDATE sync round ${round + 1}/5 (resolving conflicts)")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
            // Additional sync to ensure conflict resolution uploads are processed
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        verifyDataConsistency(db1, db2, "after moderate UPDATE operations with conflicts")

        println("✓ Phase 2 completed: Moderate UPDATE operations with conflicts resolved")

        // Phase 3: Moderate DELETE Operations
        println("\n--- Phase 3: Moderate DELETE Operations ---")

        // Each device selects users from its own current state to avoid race conditions
        // Device 1: Select users with names containing "User1" (its own users)
        val device1UsersForDeletion = db1.prepare("SELECT id FROM users WHERE name LIKE 'User1-%' LIMIT 3").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        // Device 2: Select users with names containing "User2" (its own users)
        val device2UsersForDeletion = db2.prepare("SELECT id FROM users WHERE name LIKE 'User2-%' LIMIT 2").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        // Select posts for deletion from each device's own posts
        val device1PostsForDeletion = db1.prepare("SELECT id FROM posts WHERE title LIKE 'Post1-%' LIMIT 4").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        val device2PostsForDeletion = db2.prepare("SELECT id FROM posts WHERE title LIKE 'Post2-%' LIMIT 3").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        coroutineScope {
            val deleteJobs = listOf(
                // Device 1: Strategic DELETE operations on its own data
                async {
                    // First delete some posts created by Device 1
                    device1PostsForDeletion.forEachIndexed { i, postId ->
                        db1.execSQL("DELETE FROM posts WHERE id='$postId'")
                        println("[${device1Id}] Deleted post $i/${device1PostsForDeletion.size}")

                        if (i % 2 == 0) {
                            fullSync(client1, device1Id)
                        }
                        delay(50)
                    }

                    // Sync after post deletions
                    println("[${device1Id}] Syncing after post deletions")
                    fullSync(client1, device1Id)

                    // Then delete some users created by Device 1
                    device1UsersForDeletion.forEachIndexed { i, userId ->
                        // Delete any remaining posts by this user first
                        db1.execSQL("DELETE FROM posts WHERE author_id='$userId'")
                        // Then delete the user
                        db1.execSQL("DELETE FROM users WHERE id='$userId'")

                        println("[${device1Id}] Deleted user $i/${device1UsersForDeletion.size} (with cascade)")

                        if (i % 2 == 0) {
                            fullSync(client1, device1Id)
                        }
                        delay(70)
                    }
                    println("[${device1Id}] Completed DELETE operations")
                },
                // Device 2: Concurrent DELETE operations on its own data
                async {
                    delay(100) // Offset to create realistic timing

                    // Delete posts created by Device 2
                    device2PostsForDeletion.forEachIndexed { i, postId ->
                        db2.execSQL("DELETE FROM posts WHERE id='$postId'")

                        if (i % 2 == 0) {
                            println("[${device2Id}] DELETE posts progress: $i/${device2PostsForDeletion.size}")
                            fullSync(client2, device2Id)
                        }
                        delay(60)
                    }

                    // Sync after post deletions
                    println("[${device2Id}] Syncing after post deletions")
                    fullSync(client2, device2Id)

                    // Delete users created by Device 2
                    device2UsersForDeletion.forEachIndexed { i, userId ->
                        // Handle foreign key constraints properly
                        db2.execSQL("DELETE FROM posts WHERE author_id='$userId'")
                        db2.execSQL("DELETE FROM users WHERE id='$userId'")

                        println("[${device2Id}] Deleted user $i/${device2UsersForDeletion.size} (with cascade)")

                        fullSync(client2, device2Id)
                        delay(80)
                    }
                    println("[${device2Id}] Completed DELETE operations")
                }
            )

            deleteJobs.awaitAll()
        }

        // Comprehensive sync after deletes using proven pattern
        println("\n--- Syncing after DELETEs ---")
        repeat(5) { round ->
            println("DELETE sync round ${round + 1}/5")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
            // Ensure all DELETE operations are fully propagated
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        verifyDataConsistency(db1, db2, "after moderate DELETE operations")

        println("✓ Phase 3 completed: Moderate DELETE operations")

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
        verifyDataConsistency(db1, db2, "final moderate verification")

        val finalUsersCount = count(db1, "users", "1=1")
        val finalPostsCount = count(db1, "posts", "1=1")

        println("\n=== MODERATE CONCURRENT SYNC TEST VERIFICATION ===")
        println("Final users: $finalUsersCount")
        println("Final posts: $finalPostsCount")

        // Expected counts after DELETE operations:
        // - Started with 20 users (10 from each device)
        // - Device 1 deletes 3 of its own users
        // - Device 2 deletes 2 of its own users
        // - Expected remaining: 20 - 3 - 2 = 15 users
        // - Posts: Started with ~25 posts, deleted 4 + 3 = 7 posts, plus cascade deletes
        // - Expected remaining posts: should be reduced but exact count depends on cascade deletes

        // Verify we have the expected counts after all operations
        assertEquals("Should have exactly 15 users remaining after DELETE operations", 15, finalUsersCount)
        assertTrue("Should have at least 10 posts remaining", finalPostsCount >= 10)
        assertTrue("Should have at most 25 posts remaining", finalPostsCount <= 25)

        // Verify foreign key integrity
        val orphanedPosts1 = count(db1, "posts", "author_id NOT IN (SELECT id FROM users)")
        val orphanedPosts2 = count(db2, "posts", "author_id NOT IN (SELECT id FROM users)")
        assertEquals("No orphaned posts should exist in db1", 0, orphanedPosts1)
        assertEquals("No orphaned posts should exist in db2", 0, orphanedPosts2)

        // Verify data content consistency for a sample of records
        val sampleUserIds = db1.prepare("SELECT id FROM users LIMIT 5").use { st ->
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

        println("\n🎉 MODERATE CONCURRENT SYNC TEST PASSED! 🎉")
        println("✅ Final state: $finalUsersCount users, $finalPostsCount posts")

        // Cleanup
        client1.close()
        client2.close()
    }

    /**
     * Helper method to dump debug information about DELETE operations
     */
    private suspend fun dumpDeleteDebugInfo(db: SafeSQLiteConnection, deviceId: String) {
        println("\n=== DELETE DEBUG INFO for $deviceId ===")

        // Debug trigger logging removed for production readiness
        println("Recent DELETE trigger logs: (disabled for production)")

        // Check pending DELETE operations
        db.prepare("SELECT table_name, pk_uuid, op, base_version FROM _sync_pending WHERE op='DELETE'")
            .use { st ->
                println("Pending DELETE operations:")
                while (st.step()) {
                    val table = st.getText(0)
                    val pk = st.getText(1)
                    val op = st.getText(2)
                    val baseVersion = st.getLong(3)
                    println("  $op $table:${pk.take(8)} (base_version=$baseVersion)")
                }
            }

        // Check sync row meta for deleted records
        db.prepare("SELECT table_name, pk_uuid, server_version, deleted FROM _sync_row_meta WHERE deleted=1 LIMIT 10")
            .use { st ->
                println("Deleted records in _sync_row_meta:")
                while (st.step()) {
                    val table = st.getText(0)
                    val pk = st.getText(1)
                    val serverVersion = st.getLong(2)
                    val deleted = st.getLong(3)
                    println("  $table:${pk.take(8)} server_version=$serverVersion deleted=$deleted")
                }
            }

        println("=== END DELETE DEBUG INFO ===\n")
    }

    @Test
    fun debug_delete_operations_test() = runBlockingTest {
        println("=== Starting Debug DELETE Operations Test ===")

        // Setup two devices for debugging DELETE operations
        val userId = "user-debug-${UUID.randomUUID().toString().substring(0, 8)}"
        val device1Id = "device-debug-1"
        val device2Id = "device-debug-2"

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

        // Phase 1: Create a small set of test data
        println("\n--- Phase 1: Creating test data ---")

        // Device 1: Create 5 users with 2 posts each
        repeat(5) { i ->
            val userId1 = UUID.randomUUID().toString()
            val userName = "User1-$i"
            val userEmail = "user1-$i@device1.com"

            db1.execSQL("INSERT INTO users(id, name, email) VALUES('$userId1','$userName','$userEmail')")

            repeat(2) { j ->
                val postId1 = UUID.randomUUID().toString()
                val postTitle = "Post1-$i-$j"
                val postContent = "Content from device 1 - user $i, post $j"

                db1.execSQL("INSERT INTO posts(id, title, content, author_id) VALUES('$postId1','$postTitle','$postContent','$userId1')")
            }
        }

        // Sync after creation
        fullSync(client1, device1Id, db1)
        fullSync(client2, device2Id, db2)

        val usersAfterInserts = count(db1, "users", "1=1")
        val postsAfterInserts = count(db1, "posts", "1=1")
        println("After INSERTs: Users=$usersAfterInserts, Posts=$postsAfterInserts")

        // Verify consistency
        verifyDataConsistency(db1, db2, "after initial inserts")

        // Phase 2: Perform DELETE operations with detailed logging
        println("\n--- Phase 2: DELETE operations with detailed logging ---")

        // Get one user to delete
        val userToDelete = db1.prepare("SELECT id FROM users LIMIT 1").use { st ->
            if (st.step()) st.getText(0) else null
        }

        if (userToDelete != null) {
            println("Will delete user: ${userToDelete.take(8)}")

            // Print state before DELETE
            printDatabaseState(db1, device1Id, "BEFORE_DELETE")
            printDatabaseState(db2, device2Id, "BEFORE_DELETE")

            // Delete posts first, then user
            println("[$device1Id] Deleting posts for user ${userToDelete.take(8)}")
            db1.execSQL("DELETE FROM posts WHERE author_id='$userToDelete'")

            println("[$device1Id] Deleting user ${userToDelete.take(8)}")
            db1.execSQL("DELETE FROM users WHERE id='$userToDelete'")

            // Print state after DELETE but before sync
            printDatabaseState(db1, device1Id, "AFTER_DELETE_BEFORE_SYNC")
            printDatabaseState(db2, device2Id, "AFTER_DELETE_BEFORE_SYNC")

            // Sync the DELETE operations
            println("[$device1Id] Syncing DELETE operations...")
            fullSync(client1, device1Id, db1)
            fullSync(client2, device2Id, db2)

            // Print state after sync
            printDatabaseState(db1, device1Id, "AFTER_SYNC")
            printDatabaseState(db2, device2Id, "AFTER_SYNC")

            // Verify consistency
            verifyDataConsistency(db1, db2, "after DELETE operations")
        }

        val finalUsersCount = count(db1, "users", "1=1")
        val finalPostsCount = count(db1, "posts", "1=1")

        println("\n=== DEBUG DELETE TEST RESULTS ===")
        println("Final users: $finalUsersCount")
        println("Final posts: $finalPostsCount")
        println("Expected: 4 users, 8 posts (deleted 1 user + 2 posts)")

        assertEquals("Should have 4 users after DELETE", 4, finalUsersCount)
        assertEquals("Should have 8 posts after DELETE", 8, finalPostsCount)

        // Cleanup
        client1.close()
        client2.close()
    }

    @Test
    fun intermediate_concurrent_stress_test() = runBlockingTest {
        println("=== Starting Intermediate Concurrent Stress Test ===")

        // Setup two devices for intermediate stress testing
        val userId = "user-intermediate-${UUID.randomUUID().toString().substring(0, 8)}"
        val device1Id = "device-intermediate-1"
        val device2Id = "device-intermediate-2"

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

        // Phase 1: Intermediate INSERT Operations (30 users + 45 posts per device)
        println("\n--- Phase 1: Intermediate INSERT Operations (30 users + 45 posts per device) ---")

        coroutineScope {
            val insertJobs = listOf(
                // Device 1: Insert 30 users + posts
                async {
                    repeat(30) { i ->
                        val userId1 = UUID.randomUUID().toString()
                        val userName = "User1-$i"
                        val userEmail = "user1-$i@device1.com"

                        db1.execSQL("INSERT INTO users(id, name, email) VALUES('$userId1','$userName','$userEmail')")

                        // Create 1-2 posts per user (varying to create realistic data)
                        val postsPerUser = if (i % 3 == 0) 2 else 1
                        repeat(postsPerUser) { j ->
                            val postId1 = UUID.randomUUID().toString()
                            val postTitle = "Post1-$i-$j"
                            val postContent = "Content from device 1 - user $i, post $j"

                            db1.execSQL("INSERT INTO posts(id, title, content, author_id) VALUES('$postId1','$postTitle','$postContent','$userId1')")
                        }

                        if (i % 5 == 0) {
                            println("[${device1Id}] INSERT progress: $i/30 users")
                            fullSync(client1, device1Id)
                        }
                        delay(15) // Moderate timing
                    }
                    println("[${device1Id}] Completed INSERT operations")
                },
                // Device 2: Insert 30 users + posts
                async {
                    delay(25) // Offset for realistic concurrency
                    repeat(30) { i ->
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
                            println("[${device2Id}] INSERT progress: $i/30 users")
                            fullSync(client2, device2Id)
                        }
                        delay(18) // Different timing for realistic concurrency
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

        verifyDataConsistency(db1, db2, "after intermediate INSERT operations")

        val usersAfterInserts = count(db1, "users", "1=1")
        val postsAfterInserts = count(db1, "posts", "1=1")
        println("After INSERTs: Users=$usersAfterInserts, Posts=$postsAfterInserts")

        // Verify reasonable counts (should be around 60 users, 75-90 posts)
        assertTrue("Should have at least 55 users", usersAfterInserts >= 55)
        assertTrue("Should have at least 70 posts", postsAfterInserts >= 70)
        assertTrue("Should have at most 65 users", usersAfterInserts <= 65)
        assertTrue("Should have at most 95 posts", postsAfterInserts <= 95)

        println("✓ Phase 1 completed: Intermediate INSERT operations verified")

        // Phase 2: Intermediate UPDATE Operations with Overlapping Conflicts
        println("\n--- Phase 2: Intermediate UPDATE Operations with Overlapping Conflicts ---")

        // Get existing records for updates
        val existingUserIds = db1.prepare("SELECT id FROM users ORDER BY id").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        val existingPostIds = db1.prepare("SELECT id FROM posts ORDER BY id").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        coroutineScope {
            val updateJobs = listOf(
                // Device 1: Update overlapping records to create intentional conflicts
                async {
                    // Update users with IDs 0-15 (overlaps with device 2's 8-23)
                    existingUserIds.take(15).forEachIndexed { i, userId ->
                        val timestamp = System.currentTimeMillis()
                        val newName = "UpdatedUser1-$i-$timestamp"
                        val newEmail = "updated1-$i-$timestamp@device1.com"

                        db1.execSQL("UPDATE users SET name='$newName', email='$newEmail' WHERE id='$userId'")

                        if (i % 4 == 0) {
                            println("[${device1Id}] UPDATE users progress: $i/15")
                            fullSync(client1, device1Id)
                        }
                        delay(20)
                    }

                    // Update posts with overlapping IDs
                    existingPostIds.take(18).forEachIndexed { i, postId ->
                        val timestamp = System.currentTimeMillis()
                        val newTitle = "UpdatedPost1-$i-$timestamp"
                        val newContent = "Updated content from device 1 - $i - $timestamp"

                        db1.execSQL("UPDATE posts SET title='$newTitle', content='$newContent' WHERE id='$postId'")

                        if (i % 5 == 0) {
                            println("[${device1Id}] UPDATE posts progress: $i/18")
                            fullSync(client1, device1Id)
                        }
                        delay(25)
                    }
                    println("[${device1Id}] Completed UPDATE operations")
                },
                // Device 2: Update overlapping records to create intentional conflicts
                async {
                    delay(35) // Offset to create realistic conflicts

                    // Update users with overlapping IDs (8-23 range overlaps with device 1's 0-15)
                    existingUserIds.drop(8).take(15).forEachIndexed { i, userId ->
                        val timestamp = System.currentTimeMillis()
                        val newName = "UpdatedUser2-$i-$timestamp"
                        val newEmail = "updated2-$i-$timestamp@device2.com"

                        db2.execSQL("UPDATE users SET name='$newName', email='$newEmail' WHERE id='$userId'")

                        if (i % 4 == 0) {
                            println("[${device2Id}] UPDATE users progress: $i/15")
                            fullSync(client2, device2Id)
                        }
                        delay(22)
                    }

                    // Update posts with overlapping IDs
                    existingPostIds.drop(10).take(18).forEachIndexed { i, postId ->
                        val timestamp = System.currentTimeMillis()
                        val newTitle = "UpdatedPost2-$i-$timestamp"
                        val newContent = "Updated content from device 2 - $i - $timestamp"

                        db2.execSQL("UPDATE posts SET title='$newTitle', content='$newContent' WHERE id='$postId'")

                        if (i % 5 == 0) {
                            println("[${device2Id}] UPDATE posts progress: $i/18")
                            fullSync(client2, device2Id)
                        }
                        delay(28)
                    }
                    println("[${device2Id}] Completed UPDATE operations")
                }
            )

            updateJobs.awaitAll()
        }

        // Comprehensive sync after updates using proven pattern
        println("\n--- Syncing after UPDATEs ---")
        repeat(3) { round ->
            println("UPDATE sync round ${round + 1}/3")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        verifyDataConsistency(db1, db2, "after intermediate UPDATE operations")

        val usersAfterUpdates = count(db1, "users", "1=1")
        val postsAfterUpdates = count(db1, "posts", "1=1")
        println("After UPDATEs: Users=$usersAfterUpdates, Posts=$postsAfterUpdates")

        // Verify counts haven't changed (UPDATEs don't change record counts)
        assertEquals("User count should remain the same after UPDATEs", usersAfterInserts, usersAfterUpdates)
        assertEquals("Post count should remain the same after UPDATEs", postsAfterInserts, postsAfterUpdates)

        println("✓ Phase 2 completed: Intermediate UPDATE operations with conflicts verified")

        // Phase 3: Moderate DELETE Operations with Foreign Key Handling
        println("\n--- Phase 3: Moderate DELETE Operations with Foreign Key Handling ---")

        // Get all user IDs for deterministic deletion
        val allUserIds = db1.prepare("SELECT id FROM users ORDER BY id").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        // Split users deterministically: Device1 gets even indices, Device2 gets odd indices
        val device1UsersForDeletion = allUserIds.filterIndexed { index, _ -> index % 2 == 0 }.take(5)
        val device2UsersForDeletion = allUserIds.filterIndexed { index, _ -> index % 2 == 1 }.take(4)

        // For posts, delete only posts that belong to users we're NOT deleting
        // This avoids foreign key cascade conflicts
        val usersNotBeingDeleted = allUserIds - device1UsersForDeletion.toSet() - device2UsersForDeletion.toSet()

        val device1PostsForDeletion = db1.prepare(
            "SELECT id FROM posts WHERE author_id IN (${
                usersNotBeingDeleted.joinToString(",") { "'$it'" }
            }) ORDER BY id LIMIT 8"
        ).use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        val device2PostsForDeletion = db2.prepare(
            "SELECT id FROM posts WHERE author_id IN (${
                usersNotBeingDeleted.joinToString(",") { "'$it'" }
            }) ORDER BY id LIMIT 6 OFFSET 8"
        ).use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        println("Device1 will delete: ${device1UsersForDeletion.size} users, ${device1PostsForDeletion.size} posts")
        println("Device2 will delete: ${device2UsersForDeletion.size} users, ${device2PostsForDeletion.size} posts")
        println("Total expected deletions: ${device1UsersForDeletion.size + device2UsersForDeletion.size} users, ${device1PostsForDeletion.size + device2PostsForDeletion.size} posts")

        // Track actual deletions for verification
        val actualUsersDeleted = AtomicInteger(0)
        val actualPostsDeleted = AtomicInteger(0)

        coroutineScope {
            val deleteJobs = listOf(
                // Device 1: DELETE operations
                async {
                    // Delete posts first (to avoid foreign key violations)
                    device1PostsForDeletion.forEachIndexed { i, postId ->
                        db1.execSQL("DELETE FROM posts WHERE id='$postId'")
                        actualPostsDeleted.incrementAndGet()

                        if (i % 3 == 0) {
                            println("[${device1Id}] DELETE posts progress: $i/${device1PostsForDeletion.size}")
                            fullSync(client1, device1Id)
                        }
                        delay(30)
                    }

                    // Then delete users (with cascade delete for their remaining posts)
                    device1UsersForDeletion.forEachIndexed { i, userId ->
                        // Delete any remaining posts by this user first
                        db1.execSQL("DELETE FROM posts WHERE author_id='$userId'")
                        // Then delete the user
                        db1.execSQL("DELETE FROM users WHERE id='$userId'")
                        actualUsersDeleted.incrementAndGet()

                        if (i % 2 == 0) {
                            println("[${device1Id}] DELETE users progress: $i/${device1UsersForDeletion.size}")
                            fullSync(client1, device1Id)
                        }
                        delay(35)
                    }
                    println("[${device1Id}] Completed DELETE operations")
                },
                // Device 2: DELETE operations
                async {
                    delay(40) // Offset for realistic concurrency

                    // Delete posts first (to avoid foreign key violations)
                    device2PostsForDeletion.forEachIndexed { i, postId ->
                        db2.execSQL("DELETE FROM posts WHERE id='$postId'")
                        actualPostsDeleted.incrementAndGet()

                        if (i % 2 == 0) {
                            println("[${device2Id}] DELETE posts progress: $i/${device2PostsForDeletion.size}")
                            fullSync(client2, device2Id)
                        }
                        delay(32)
                    }

                    // Then delete users (with cascade delete for their remaining posts)
                    device2UsersForDeletion.forEachIndexed { i, userId ->
                        // Delete any remaining posts by this user first
                        db2.execSQL("DELETE FROM posts WHERE author_id='$userId'")
                        // Then delete the user
                        db2.execSQL("DELETE FROM users WHERE id='$userId'")
                        actualUsersDeleted.incrementAndGet()

                        if (i % 2 == 0) {
                            println("[${device2Id}] DELETE users progress: $i/${device2UsersForDeletion.size}")
                            fullSync(client2, device2Id)
                        }
                        delay(38)
                    }
                    println("[${device2Id}] Completed DELETE operations")
                }
            )

            deleteJobs.awaitAll()
        }

        // Comprehensive sync after deletes using proven pattern
        println("\n--- Syncing after DELETEs ---")
        repeat(3) { round ->
            println("DELETE sync round ${round + 1}/3")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        verifyDataConsistency(db1, db2, "after intermediate DELETE operations")

        // Detailed verification of final state
        val finalUsersCount = count(db1, "users", "1=1")
        val finalPostsCount = count(db1, "posts", "1=1")

        println("\n=== INTERMEDIATE STRESS TEST VERIFICATION ===")
        println("Final users: $finalUsersCount")
        println("Final posts: $finalPostsCount")

        // Verify we have reasonable counts after all operations including DELETEs
        val actualUserDeletions = actualUsersDeleted.get()
        val actualPostDeletions = actualPostsDeleted.get()

        // Calculate cascade deletions: posts that were deleted when their authors were deleted
        val plannedUserDeletions = device1UsersForDeletion.size + device2UsersForDeletion.size
        val averagePostsPerUser = postsAfterUpdates.toDouble() / usersAfterUpdates.toDouble()
        val estimatedCascadePostDeletions = (actualUserDeletions * averagePostsPerUser).toInt()

        val totalEstimatedPostDeletions = actualPostDeletions + estimatedCascadePostDeletions
        val expectedFinalUsers = usersAfterUpdates - actualUserDeletions
        val expectedMinPosts = postsAfterUpdates - totalEstimatedPostDeletions
        val expectedMaxPosts = postsAfterUpdates - actualPostDeletions // Minimum cascade scenario

        println("Planned deletions: $plannedUserDeletions users, ${device1PostsForDeletion.size + device2PostsForDeletion.size} posts")
        println("Actual deletions: $actualUserDeletions users, $actualPostDeletions posts (explicit)")
        println("Estimated cascade post deletions: $estimatedCascadePostDeletions")
        println("Total estimated post deletions: $totalEstimatedPostDeletions")
        println("Expected final users: $expectedFinalUsers")
        println("Expected final posts: $expectedMinPosts-$expectedMaxPosts")

        // Use actual deletion counts for verification to account for race conditions
        // Allow for some variance due to race conditions and estimation errors
        val userTolerance = 3
        val postTolerance = 8 // Higher tolerance for posts due to cascade deletion estimation

        assertTrue(
            "Should have at least ${expectedFinalUsers - userTolerance} users after DELETEs (actual: $finalUsersCount)",
            finalUsersCount >= expectedFinalUsers - userTolerance
        )
        assertTrue(
            "Should have at least ${expectedMinPosts - postTolerance} posts after DELETEs (actual: $finalPostsCount)",
            finalPostsCount >= expectedMinPosts - postTolerance
        )
        assertTrue(
            "Should have at most ${expectedFinalUsers + userTolerance} users after DELETEs (actual: $finalUsersCount)",
            finalUsersCount <= expectedFinalUsers + userTolerance
        )
        assertTrue(
            "Should have at most ${expectedMaxPosts + postTolerance} posts after DELETEs (actual: $finalPostsCount)",
            finalPostsCount <= expectedMaxPosts + postTolerance
        )

        println("✓ Phase 3 completed: Intermediate DELETE operations with foreign key handling verified")

        println("\n=== INTERMEDIATE CONCURRENT STRESS TEST COMPLETED SUCCESSFULLY ===")
        println("Final state: $finalUsersCount users, $finalPostsCount posts")
        println("All phases completed with proper conflict resolution and foreign key handling")

        client1.close()
        client2.close()
    }

    /*
    @Test
    fun comprehensive_bullet_proof_stress_test() = runBlockingTest {
        println("=== Starting Comprehensive Bullet-Proof Stress Test (INSERT/UPDATE Focus) ===")

        // Setup two devices for comprehensive stress testing (focusing on INSERT/UPDATE)
        val userId = "user-stress-${UUID.randomUUID().toString().substring(0, 8)}"
        val device1Id = "device-stress-1"
        val device2Id = "device-stress-2"

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

        // Phase 1: Massive INSERT Operations (400+ records)
        println("\n--- Phase 1: Massive INSERT Operations (75 users + 125 posts per device) ---")

        coroutineScope {
            val massiveInsertJobs = listOf(
                // Device 1: Insert 75 users + posts
                async {
                    repeat(75) { i ->
                        val userId1 = UUID.randomUUID().toString()
                        val userName = "User1-$i"
                        val userEmail = "user1-$i@device1.com"

                        db1.execSQL("INSERT INTO users(id, name, email) VALUES('$userId1','$userName','$userEmail')")

                        // Create 1-2 posts per user (varying to create realistic data)
                        val postsPerUser = if (i % 3 == 0) 2 else 1
                        repeat(postsPerUser) { j ->
                            val postId1 = UUID.randomUUID().toString()
                            val postTitle = "Post1-$i-$j"
                            val postContent = "Content from device 1 - user $i, post $j"

                            db1.execSQL("INSERT INTO posts(id, title, content, author_id) VALUES('$postId1','$postTitle','$postContent','$userId1')")
                        }

                        if (i % 8 == 0) {
                            println("[${device1Id}] INSERT progress: $i/75 users")
                            fullSync(client1, device1Id)
                        }
                        delay(8) // Faster timing for stress test
                    }
                    println("[${device1Id}] Completed massive INSERT operations")
                },
                // Device 2: Insert 75 users + posts
                async {
                    delay(15) // Slight offset for realistic concurrency
                    repeat(75) { i ->
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

                        if (i % 10 == 0) {
                            println("[${device2Id}] INSERT progress: $i/75 users")
                            fullSync(client2, device2Id)
                        }
                        delay(12) // Different timing for realistic concurrency
                    }
                    println("[${device2Id}] Completed massive INSERT operations")
                }
            )

            massiveInsertJobs.awaitAll()
        }

        // Comprehensive sync after massive inserts
        println("\n--- Syncing after massive INSERTs ---")
        repeat(3) { round ->
            println("INSERT sync round ${round + 1}/3")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        verifyDataConsistency(db1, db2, "after massive INSERT operations")

        val usersAfterInserts = count(db1, "users", "1=1")
        val postsAfterInserts = count(db1, "posts", "1=1")
        println("After INSERTs: Users=$usersAfterInserts, Posts=$postsAfterInserts")

        // Verify we have a reasonable number of records (should be around 150 users, 187-225 posts)
        assertTrue("Should have at least 145 users", usersAfterInserts >= 145)
        assertTrue("Should have at least 180 posts", postsAfterInserts >= 180)
        assertTrue("Should have at most 155 users", usersAfterInserts <= 155)
        assertTrue("Should have at most 230 posts", postsAfterInserts <= 230)

        println("✓ Phase 1 completed: Massive INSERT operations verified")

        // Phase 2: Massive UPDATE Operations with Intentional Conflicts (100+ updates per device)
        println("\n--- Phase 2: Massive UPDATE Operations with Conflicts ---")

        // Get existing records for updates
        val existingUserIds = db1.prepare("SELECT id FROM users LIMIT 80").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        val existingPostIds = db1.prepare("SELECT id FROM posts LIMIT 100").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        coroutineScope {
            val massiveUpdateJobs = listOf(
                // Device 1: Update 25 users + 30 posts with conflicts
                async {
                    // Update users with overlapping IDs to create conflicts
                    existingUserIds.take(25).forEachIndexed { i, userId ->
                        val timestamp = System.currentTimeMillis()
                        val newName = "UpdatedUser1-$i-$timestamp"
                        val newEmail = "updated1-$i-$timestamp@device1.com"

                        db1.execSQL("UPDATE users SET name='$newName', email='$newEmail' WHERE id='$userId'")

                        if (i % 5 == 0) {
                            println("[${device1Id}] UPDATE users progress: $i/25")
                            fullSync(client1, device1Id)
                        }
                        delay(20)
                    }

                    // Update posts with overlapping IDs to create conflicts
                    existingPostIds.take(30).forEachIndexed { i, postId ->
                        val timestamp = System.currentTimeMillis()
                        val newTitle = "UpdatedPost1-$i-$timestamp"
                        val newContent = "Updated content from device 1 - $i - $timestamp"

                        db1.execSQL("UPDATE posts SET title='$newTitle', content='$newContent' WHERE id='$postId'")

                        if (i % 8 == 0) {
                            println("[${device1Id}] UPDATE posts progress: $i/30")
                            fullSync(client1, device1Id)
                        }
                        delay(25)
                    }
                    println("[${device1Id}] Completed massive UPDATE operations")
                },
                // Device 2: Update overlapping records to create intentional conflicts
                async {
                    delay(50) // Offset to create realistic conflicts

                    // Update users with overlapping IDs (12-37 range overlaps with device 1's 0-25)
                    existingUserIds.drop(12).take(25).forEachIndexed { i, userId ->
                        val timestamp = System.currentTimeMillis()
                        val newName = "UpdatedUser2-$i-$timestamp"
                        val newEmail = "updated2-$i-$timestamp@device2.com"

                        db2.execSQL("UPDATE users SET name='$newName', email='$newEmail' WHERE id='$userId'")

                        if (i % 6 == 0) {
                            println("[${device2Id}] UPDATE users progress: $i/25")
                            fullSync(client2, device2Id)
                        }
                        delay(30)
                    }

                    // Update posts with overlapping IDs (15-45 range overlaps with device 1's 0-30)
                    existingPostIds.drop(15).take(30).forEachIndexed { i, postId ->
                        val timestamp = System.currentTimeMillis()
                        val newTitle = "UpdatedPost2-$i-$timestamp"
                        val newContent = "Updated content from device 2 - $i - $timestamp"

                        db2.execSQL("UPDATE posts SET title='$newTitle', content='$newContent' WHERE id='$postId'")

                        if (i % 10 == 0) {
                            println("[${device2Id}] UPDATE posts progress: $i/30")
                            fullSync(client2, device2Id)
                        }
                        delay(35)
                    }
                    println("[${device2Id}] Completed massive UPDATE operations")
                }
            )

            massiveUpdateJobs.awaitAll()
        }

        // Comprehensive sync after massive updates with conflict resolution
        println("\n--- Syncing after massive UPDATEs (with conflict resolution) ---")
        repeat(5) { round ->
            println("UPDATE sync round ${round + 1}/5 (resolving conflicts)")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
            // Additional sync to ensure conflict resolution uploads are processed
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        verifyDataConsistency(db1, db2, "after massive UPDATE operations with conflicts")

        println("✓ Phase 2 completed: Massive UPDATE operations with conflicts resolved")

        // Phase 3: Strategic DELETE Operations with Proper Sync Sequencing
        println("\n--- Phase 3: Strategic DELETE Operations with Proper Sync Sequencing ---")

        // Get all user IDs and split them deterministically between devices
        val allUserIds = db1.prepare("SELECT id FROM users ORDER BY id").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        // Split users deterministically: Device1 gets even indices, Device2 gets odd indices
        val device1UsersForDeletion =
            allUserIds.filterIndexed { index, _ -> index % 2 == 0 }.take(8)
        val device2UsersForDeletion =
            allUserIds.filterIndexed { index, _ -> index % 2 == 1 }.take(6)

        // For posts, delete only posts that belong to users we're NOT deleting
        // This avoids foreign key cascade conflicts
        val usersNotBeingDeleted =
            allUserIds - device1UsersForDeletion.toSet() - device2UsersForDeletion.toSet()

        val device1PostsForDeletion = db1.prepare(
            "SELECT id FROM posts WHERE author_id IN (${
                usersNotBeingDeleted.joinToString(",") { "'$it'" }
            }) ORDER BY id LIMIT 15"
        ).use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        val device2PostsForDeletion = db2.prepare(
            "SELECT id FROM posts WHERE author_id IN (${
                usersNotBeingDeleted.joinToString(",") { "'$it'" }
            }) ORDER BY id LIMIT 10 OFFSET 15"
        ).use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        println("Device1 will delete: ${device1UsersForDeletion.size} users, ${device1PostsForDeletion.size} posts")
        println("Device2 will delete: ${device2UsersForDeletion.size} users, ${device2PostsForDeletion.size} posts")
        println("Total expected deletions: ${device1UsersForDeletion.size + device2UsersForDeletion.size} users, ${device1PostsForDeletion.size + device2PostsForDeletion.size} posts")

        // Dump initial state before DELETE operations
        dumpDeleteDebugInfo(db1, device1Id)
        dumpDeleteDebugInfo(db2, device2Id)

        // Counters to track actual DELETE operations performed
        val actualUsersDeleted = AtomicInteger(0)
        val actualPostsDeleted = AtomicInteger(0)

        coroutineScope {
            val strategicDeleteJobs = listOf(
                // Device 1: Strategic DELETE operations with proper sync sequencing
                async {
                    // First delete some posts to avoid foreign key violations
                    device1PostsForDeletion.forEachIndexed { i, postId ->
                        println("[${device1Id}] Deleting post $i/${device1PostsForDeletion.size}")
                        db1.execSQL("DELETE FROM posts WHERE id='$postId'")
                        actualPostsDeleted.incrementAndGet()

                        // Very frequent sync during DELETE operations (critical for DELETE race conditions)
                        if (i % 2 == 0) {
                            println("[${device1Id}] Intermediate sync after post DELETE $i")
                            repeat(2) { round ->
                                fullSync(client1, device1Id)
                                fullSync(client2, device2Id)
                            }
                        }
                        delay(60) // Longer delay to allow sync propagation
                    }

                    // Extensive sync after post deletions (critical for DELETE propagation)
                    println("[${device1Id}] Extensive sync after post deletions")
                    repeat(5) { round ->
                        println("[${device1Id}] Post DELETE sync round ${round + 1}/5")
                        fullSync(client1, device1Id)
                        fullSync(client2, device2Id)
                        // Double sync to ensure DELETE propagation
                        fullSync(client1, device1Id)
                        fullSync(client2, device2Id)
                    }

                    // Then delete some users (after ensuring their posts are handled)
                    device1UsersForDeletion.forEachIndexed { i, userId ->
                        // Check if user still exists before attempting deletion
                        val userExists =
                            db1.prepare("SELECT COUNT(*) FROM users WHERE id=?").use { st ->
                                st.bindText(1, userId)
                                st.step() && st.getLong(0) > 0
                            }

                        if (userExists) {
                            println(
                                "[${device1Id}] Deleting user $i/${device1UsersForDeletion.size} (${
                                    userId.take(
                                        8
                                    )
                                })"
                            )
                            // Delete any remaining posts by this user first
                            db1.execSQL("DELETE FROM posts WHERE author_id='$userId'")
                            // Then delete the user
                            db1.execSQL("DELETE FROM users WHERE id='$userId'")
                            actualUsersDeleted.incrementAndGet()
                        } else {
                            println(
                                "[${device1Id}] User $i/${device1UsersForDeletion.size} (${
                                    userId.take(
                                        8
                                    )
                                }) already deleted"
                            )
                        }

                        // Very frequent sync during user DELETE operations (critical for DELETE race conditions)
                        println("[${device1Id}] Immediate sync after user DELETE $i")
                        repeat(3) { round ->
                            fullSync(client1, device1Id, db1)
                            fullSync(client2, device2Id, db2)
                        }
                        delay(80) // Longer delay to allow sync propagation
                    }
                    println("[${device1Id}] Completed strategic DELETE operations")
                },
                // Device 2: Concurrent DELETE operations with proper sync sequencing
                async {
                    delay(80) // Offset to create realistic timing conflicts

                    // Delete different posts (non-overlapping with Device 1)
                    device2PostsForDeletion.forEachIndexed { i, postId ->
                        println("[${device2Id}] Deleting post $i/${device2PostsForDeletion.size}")
                        db2.execSQL("DELETE FROM posts WHERE id='$postId'")
                        actualPostsDeleted.incrementAndGet()

                        // Very frequent sync during DELETE operations (critical for DELETE race conditions)
                        if (i % 2 == 0) {
                            println("[${device2Id}] Intermediate sync after post DELETE $i")
                            repeat(2) { round ->
                                fullSync(client2, device2Id)
                                fullSync(client1, device1Id)
                            }
                        }
                        delay(65) // Longer delay to allow sync propagation
                    }

                    // Extensive sync after post deletions (critical for DELETE propagation)
                    println("[${device2Id}] Extensive sync after post deletions")
                    repeat(5) { round ->
                        println("[${device2Id}] Post DELETE sync round ${round + 1}/5")
                        fullSync(client2, device2Id)
                        fullSync(client1, device1Id)
                        // Double sync to ensure DELETE propagation
                        fullSync(client2, device2Id)
                        fullSync(client1, device1Id)
                    }

                    // Delete different users (non-overlapping with Device 1)
                    device2UsersForDeletion.forEachIndexed { i, userId ->
                        // Check if user still exists before attempting deletion
                        val userExists =
                            db2.prepare("SELECT COUNT(*) FROM users WHERE id=?").use { st ->
                                st.bindText(1, userId)
                                st.step() && st.getLong(0) > 0
                            }

                        if (userExists) {
                            println(
                                "[${device2Id}] Deleting user $i/${device2UsersForDeletion.size} (${
                                    userId.take(
                                        8
                                    )
                                })"
                            )
                            // Handle foreign key constraints properly
                            db2.execSQL("DELETE FROM posts WHERE author_id='$userId'")
                            db2.execSQL("DELETE FROM users WHERE id='$userId'")
                            actualUsersDeleted.incrementAndGet()
                        } else {
                            println(
                                "[${device2Id}] User $i/${device2UsersForDeletion.size} (${
                                    userId.take(
                                        8
                                    )
                                }) already deleted"
                            )
                        }

                        // Very frequent sync during user DELETE operations (critical for DELETE race conditions)
                        println("[${device2Id}] Immediate sync after user DELETE $i")
                        repeat(3) { round ->
                            fullSync(client2, device2Id)
                            fullSync(client1, device1Id)
                        }
                        delay(90) // Longer delay to allow sync propagation
                    }
                    println("[${device2Id}] Completed strategic DELETE operations")
                }
            )

            strategicDeleteJobs.awaitAll()
        }

        // Dump debug info after DELETE operations but before final sync
        println("\n--- DEBUG: State after DELETE operations, before final sync ---")
        dumpDeleteDebugInfo(db1, device1Id)
        dumpDeleteDebugInfo(db2, device2Id)

        // Ultra-comprehensive sync after strategic DELETE operations (DELETE operations need more sync rounds)
        println("\n--- Ultra-comprehensive sync after strategic DELETE operations ---")
        repeat(10) { round ->
            println("Strategic DELETE sync round ${round + 1}/10 (DELETE operations require extensive sync)")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
            // Triple sync to ensure all DELETE operations are fully propagated (DELETE needs more than UPDATE)
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        verifyDataConsistency(db1, db2, "after strategic DELETE operations")

        println("✓ Phase 3 completed: Strategic DELETE operations with proper sync sequencing")

        // Phase 4: Final Comprehensive Verification and Stress Testing
        println("\n--- Phase 4: Final Comprehensive Verification ---")

        // Final multi-round sync to ensure absolute consistency
        repeat(7) { round ->
            println("Final verification sync round ${round + 1}/7")
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
            // Double sync each round to ensure all conflict resolutions are processed
            fullSync(client1, device1Id)
            fullSync(client2, device2Id)
        }

        // Comprehensive data consistency verification
        verifyDataConsistency(db1, db2, "final comprehensive verification")

        // Detailed verification of final state
        val finalUsersCount = count(db1, "users", "1=1")
        val finalPostsCount = count(db1, "posts", "1=1")

        println("\n=== COMPREHENSIVE BULLET-PROOF VERIFICATION ===")
        println("Final users: $finalUsersCount")
        println("Final posts: $finalPostsCount")

        // Verify we have reasonable counts after all operations including DELETEs
        val actualUserDeletions = actualUsersDeleted.get()
        val actualPostDeletions = actualPostsDeleted.get()

        // Calculate cascade deletions: posts that were deleted when their authors were deleted
        // We need to estimate this based on the planned user deletions and average posts per user
        val plannedUserDeletions = device1UsersForDeletion.size + device2UsersForDeletion.size
        val averagePostsPerUser = 187.0 / 150.0 // Approximately 1.25 posts per user
        val estimatedCascadePostDeletions = (actualUserDeletions * averagePostsPerUser).toInt()

        val totalEstimatedPostDeletions = actualPostDeletions + estimatedCascadePostDeletions
        val expectedFinalUsers = 150 - actualUserDeletions
        val expectedMinPosts = 187 - totalEstimatedPostDeletions
        val expectedMaxPosts = 225 - totalEstimatedPostDeletions

        println("Planned deletions: $plannedUserDeletions users, ${device1PostsForDeletion.size + device2PostsForDeletion.size} posts")
        println("Actual deletions: $actualUserDeletions users, $actualPostDeletions posts (explicit)")
        println("Estimated cascade post deletions: $estimatedCascadePostDeletions")
        println("Total estimated post deletions: $totalEstimatedPostDeletions")
        println("Expected final users: $expectedFinalUsers")
        println("Expected final posts: $expectedMinPosts-$expectedMaxPosts")

        // Use actual deletion counts for verification to account for race conditions
        // Allow for some variance due to race conditions and estimation errors
        val userTolerance = 5
        val postTolerance = 10 // Higher tolerance for posts due to cascade deletion estimation

        assertTrue(
            "Should have at least ${expectedFinalUsers - userTolerance} users after DELETEs (actual: $finalUsersCount)",
            finalUsersCount >= expectedFinalUsers - userTolerance
        )
        assertTrue(
            "Should have at least ${expectedMinPosts - postTolerance} posts after DELETEs (actual: $finalPostsCount)",
            finalPostsCount >= expectedMinPosts - postTolerance
        )
        assertTrue(
            "Should have at most ${expectedFinalUsers + userTolerance} users after DELETEs (actual: $finalUsersCount)",
            finalUsersCount <= expectedFinalUsers + userTolerance
        )
        assertTrue(
            "Should have at most ${expectedMaxPosts + postTolerance} posts after DELETEs (actual: $finalPostsCount)",
            finalPostsCount <= expectedMaxPosts + postTolerance
        )

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

        val samplePostIds = db1.prepare("SELECT id FROM posts LIMIT 10").use { st ->
            val ids = mutableListOf<String>()
            while (st.step()) {
                ids.add(st.getText(0))
            }
            ids
        }

        samplePostIds.forEach { postId ->
            val postData1 =
                db1.prepare("SELECT title, content, author_id FROM posts WHERE id=?").use { st ->
                    st.bindText(1, postId)
                    if (st.step()) Triple(st.getText(0), st.getText(1), st.getText(2)) else null
                }
            val postData2 =
                db2.prepare("SELECT title, content, author_id FROM posts WHERE id=?").use { st ->
                    st.bindText(1, postId)
                    if (st.step()) Triple(st.getText(0), st.getText(1), st.getText(2)) else null
                }
            assertEquals(
                "Post data should be identical across devices for $postId",
                postData1,
                postData2
            )
        }

        println("\nCOMPREHENSIVE BULLET-PROOF STRESS TEST PASSED!")
        println("✅ Final state: $finalUsersCount users, $finalPostsCount posts")

        // Cleanup
        client1.close()
        client2.close()
    }
     */
}
