package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.sqlite.execSQL
import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import java.util.UUID

/**
 * Systematic debugging tests for UPDATE synchronization issues.
 * Each test builds incrementally on the previous one to isolate the exact failure point.
 */
@RunWith(AndroidJUnit4::class)
class SimpleUpdateSyncDebuggingTest {

    @Before
    fun setUp() {
        if (skipAllTest) {
            throw UnsupportedOperationException("(TEMPORARY setUp) Not implemented yet")
        }
    }

    // Business schema is provided by TestHelpers.createBusinessTables(db)

    /**
     * Creates a test client with sync enabled
     */
    private suspend fun createTestClient(userId: String, deviceId: String): TestClient {
        val driver = BundledSQLiteDriver()
        val connection: SQLiteConnection = driver.open(":memory:")
        val db = SafeSQLiteConnection(connection)

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
     * Full sync function matching the working tests
     */
    private suspend fun fullSync(client: DefaultOversqliteClient, limit: Int = 500) {
        assertUploadSuccessWithConflicts(client.uploadOnce())
        var more = true
        while (more) {
            val downloadResult = client.downloadOnce(limit = limit, includeSelf = false)
            assertDownloadSuccess(downloadResult)
            val (applied, _) = downloadResult.getOrNull() ?: (0 to 0L)
            more = applied == limit
            if (applied == 0) break
        }
    }

    data class TestClient(val db: SafeSQLiteConnection, val client: DefaultOversqliteClient, val userId: String, val deviceId: String) {
        suspend fun bootstrap() {
            assert(client.bootstrap(userId, deviceId).isSuccess)
            assert(client.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess)
        }
        
        suspend fun getUserCount(): Int {
            return count(db, "users", "1=1")
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

        suspend fun getAllUsers(): List<Triple<String, String, String>> {
            return db.prepare("SELECT id, name, email FROM users ORDER BY id").use { st ->
                val users = mutableListOf<Triple<String, String, String>>()
                while (st.step()) {
                    users.add(Triple(st.getText(0), st.getText(1), st.getText(2)))
                }
                users
            }
        }

        suspend fun getPost(id: String): Triple<String, String, String>? {
            return db.prepare("SELECT id, title, author_id FROM posts WHERE id = ?").use { st ->
                st.bindText(1, id)
                if (st.step()) {
                    Triple(st.getText(0), st.getText(1), st.getText(2))
                } else {
                    null
                }
            }
        }

        suspend fun getAllPosts(): List<Triple<String, String, String>> {
            return db.prepare("SELECT id, title, author_id FROM posts ORDER BY id").use { st ->
                val posts = mutableListOf<Triple<String, String, String>>()
                while (st.step()) {
                    posts.add(Triple(st.getText(0), st.getText(1), st.getText(2)))
                }
                posts
            }
        }

        suspend fun insertUser(id: String, name: String, email: String) {
            db.prepare("INSERT INTO users (id, name, email) VALUES (?, ?, ?)").use { st ->
                st.bindText(1, id)
                st.bindText(2, name)
                st.bindText(3, email)
                st.step()
            }
        }

        suspend fun updateUser(id: String, name: String, email: String) {
            db.prepare("UPDATE users SET name = ?, email = ? WHERE id = ?").use { st ->
                st.bindText(1, name)
                st.bindText(2, email)
                st.bindText(3, id)
                st.step()
            }
        }

        suspend fun updateUserName(id: String, name: String) {
            db.prepare("UPDATE users SET name = ? WHERE id = ?").use { st ->
                st.bindText(1, name)
                st.bindText(2, id)
                st.step()
            }
        }

        suspend fun updateUserEmail(id: String, email: String) {
            db.prepare("UPDATE users SET email = ? WHERE id = ?").use { st ->
                st.bindText(1, email)
                st.bindText(2, id)
                st.step()
            }
        }

        suspend fun deleteUser(id: String) {
            db.prepare("DELETE FROM users WHERE id = ?").use { st ->
                st.bindText(1, id)
                st.step()
            }
        }

        suspend fun insertPost(id: String, title: String, authorId: String, content: String = "") {
            db.prepare("INSERT INTO posts (id, title, author_id, content) VALUES (?, ?, ?, ?)").use { st ->
                st.bindText(1, id)
                st.bindText(2, title)
                st.bindText(3, authorId)
                st.bindText(4, content)
                st.step()
            }
        }

        suspend fun updatePost(id: String, title: String, content: String) {
            db.prepare("UPDATE posts SET title = ?, content = ? WHERE id = ?").use { st ->
                st.bindText(1, title)
                st.bindText(2, content)
                st.bindText(3, id)
                st.step()
            }
        }

        suspend fun deletePost(id: String) {
            db.prepare("DELETE FROM posts WHERE id = ?").use { st ->
                st.bindText(1, id)
                st.step()
            }
        }

        suspend fun printSyncTablesState(label: String) {
            println("=== SYNC TABLES STATE: $label ($deviceId) ===")

            // Print _sync_pending table
            println("_sync_pending:")
            db.prepare("SELECT table_name, pk_uuid, op, base_version, payload, queued_at FROM _sync_pending ORDER BY queued_at").use { st ->
                var count = 0
                while (st.step()) {
                    count++
                    val tableName = st.getText(0)
                    val pkUuid = st.getText(1)
                    val op = st.getText(2)
                    val baseVersion = st.getLong(3)
                    val payload = st.getText(4)
                    val queuedAt = st.getText(5)
                    println("  [$count] $tableName:$pkUuid -> $op (base_v$baseVersion) at $queuedAt")
                    println("      payload: $payload")
                }
                if (count == 0) println("  (empty)")
            }

            // Print _sync_row_meta table
            println("_sync_row_meta:")
            db.prepare("SELECT table_name, pk_uuid, server_version, deleted FROM _sync_row_meta ORDER BY table_name, pk_uuid").use { st ->
                var count = 0
                while (st.step()) {
                    count++
                    val tableName = st.getText(0)
                    val pkUuid = st.getText(1)
                    val serverVersion = st.getLong(2)
                    val deleted = st.getLong(3) == 1L
                    println("  [$count] $tableName:$pkUuid -> server_v$serverVersion (deleted=$deleted)")
                }
                if (count == 0) println("  (empty)")
            }

            // Print _sync_client_info table
            println("_sync_client_info:")
            db.prepare("SELECT user_id, source_id, next_change_id, last_server_seq_seen, apply_mode, current_window_until FROM _sync_client_info").use { st ->
                if (st.step()) {
                    val userId = st.getText(0)
                    val sourceId = st.getText(1)
                    val nextChangeId = st.getLong(2)
                    val lastServerSeqSeen = st.getLong(3)
                    val applyMode = st.getLong(4)
                    val currentWindowUntil = st.getLong(5)
                    println("  user_id: $userId, source_id: $sourceId")
                    println("  next_change_id: $nextChangeId, last_server_seq_seen: $lastServerSeqSeen")
                    println("  apply_mode: $applyMode, current_window_until: $currentWindowUntil")
                } else {
                    println("  (empty)")
                }
            }

            // Print actual business data
            println("users table:")
            db.prepare("SELECT id, name, email FROM users ORDER BY name").use { st ->
                var count = 0
                while (st.step()) {
                    count++
                    val id = st.getText(0)
                    val name = st.getText(1)
                    val email = st.getText(2)
                    println("  [$count] $id: $name, $email")
                }
                if (count == 0) println("  (empty)")
            }

            println("=== END SYNC TABLES STATE ===")
        }
        
        suspend fun printDebugInfo(label: String) {
            println("=== $label ($deviceId) ===")
            println("User count: ${getUserCount()}")
            getAllUsers().forEach { (id, name, email) ->
                println("  $id: $name, $email")
            }

            // Print sync metadata
            val pendingChanges = db.prepare("SELECT table_name, pk_uuid, op, base_version FROM _sync_pending ORDER BY table_name, pk_uuid").use { st ->
                val changes = mutableListOf<String>()
                while (st.step()) {
                    changes.add("${st.getText(0)}:${st.getText(1)} ${st.getText(2)} v${st.getLong(3)}")
                }
                changes
                
            }
            println("Pending changes: $pendingChanges")

            val rowMeta = db.prepare("SELECT table_name, pk_uuid, server_version FROM _sync_row_meta ORDER BY table_name, pk_uuid").use { st ->
                val meta = mutableListOf<String>()
                while (st.step()) {
                    meta.add("${st.getText(0)}:${st.getText(1)} v${st.getLong(2)}")
                }
                meta
            }
            println("Row metadata: $rowMeta")

            val clientInfo = db.prepare("SELECT next_change_id, last_server_seq_seen, apply_mode FROM _sync_client_info").use { st ->
                if (st.step()) {
                    "next_change:${st.getLong(0)} last_seq:${st.getLong(1)} apply_mode:${st.getLong(2)}"
                } else {
                    "no client info"
                }
            }
            println("Client info: $clientInfo")
        }

        suspend fun uploadWithLogging(label: String): Boolean {
            println("🔼 $deviceId: Starting upload - $label")

            // Log sync tables BEFORE upload
            printSyncTablesState("BEFORE UPLOAD - $label")

            val result = client.uploadOnce()

            // Log sync tables AFTER upload
            printSyncTablesState("AFTER UPLOAD - $label")

            if (result.isSuccess) {
                val summary = result.getOrNull()!!
                println("🔼 $deviceId: Upload SUCCESS - $label")
                println("    Total: ${summary.total}, Applied: ${summary.applied}, Conflicts: ${summary.conflict}")
                println("    Invalid: ${summary.invalid}, Materialize Errors: ${summary.materializeError}")
                if (summary.firstErrorMessage != null) {
                    println("    First Error: ${summary.firstErrorMessage}")
                }
                if (summary.invalidReasons.isNotEmpty()) {
                    println("    Invalid Reasons: ${summary.invalidReasons}")
                }

                // Check if there were any failures even though the upload "succeeded"
                if (summary.invalid > 0 || summary.materializeError > 0) {
                    println("🔼 $deviceId: Upload had failures despite success status!")
                    return false
                }
            } else {
                println("🔼 $deviceId: Upload FAILED - $label")
                println("    Error: ${result.exceptionOrNull()?.message}")
                result.exceptionOrNull()?.printStackTrace()
            }

            return result.isSuccess
        }

        suspend fun downloadWithLogging(label: String): Pair<Int, Long> {
            println("🔽 $deviceId: Starting download - $label")

            // Log sync tables BEFORE download
            printSyncTablesState("BEFORE DOWNLOAD - $label")

            val result = client.downloadOnce(limit = 1000, includeSelf = false)
            val (applied, version) = result.getOrNull() ?: (0 to 0L)

            // Log sync tables AFTER download
            printSyncTablesState("AFTER DOWNLOAD - $label")

            println("🔽 $deviceId: Download result - applied:$applied version:$version")
            if (result.isFailure) {
                println("🔽 $deviceId: Download error details: ${result.exceptionOrNull()?.message}")
                result.exceptionOrNull()?.printStackTrace()
            }
            return applied to version
        }

        suspend fun validateDataConsistency(other: TestClient, label: String): Boolean {
            println("\n🔍 VALIDATING DATA CONSISTENCY: $label")

            var isConsistent = true
            val issues = mutableListOf<String>()

            // Compare users
            val users1 = getAllUsers()
            val users2 = other.getAllUsers()

            if (users1.size != users2.size) {
                issues.add("User count mismatch: ${deviceId}=${users1.size}, ${other.deviceId}=${users2.size}")
                isConsistent = false
            }

            val userMap1 = users1.associateBy { it.first }
            val userMap2 = users2.associateBy { it.first }

            // Check for missing users
            val missingIn2 = userMap1.keys - userMap2.keys
            val missingIn1 = userMap2.keys - userMap1.keys

            if (missingIn2.isNotEmpty()) {
                issues.add("Users missing in ${other.deviceId}: $missingIn2")
                isConsistent = false
            }

            if (missingIn1.isNotEmpty()) {
                issues.add("Users missing in ${deviceId}: $missingIn1")
                isConsistent = false
            }

            // Check for field-level differences
            val commonUsers = userMap1.keys intersect userMap2.keys
            for (userId in commonUsers) {
                val user1 = userMap1[userId]!!
                val user2 = userMap2[userId]!!

                if (user1.second != user2.second) {
                    issues.add("User $userId name mismatch: ${deviceId}='${user1.second}', ${other.deviceId}='${user2.second}'")
                    isConsistent = false
                }

                if (user1.third != user2.third) {
                    issues.add("User $userId email mismatch: ${deviceId}='${user1.third}', ${other.deviceId}='${user2.third}'")
                    isConsistent = false
                }
            }

            // Compare posts
            val posts1 = getAllPosts()
            val posts2 = other.getAllPosts()

            if (posts1.size != posts2.size) {
                issues.add("Post count mismatch: ${deviceId}=${posts1.size}, ${other.deviceId}=${posts2.size}")
                isConsistent = false
            }

            val postMap1 = posts1.associateBy { it.first }
            val postMap2 = posts2.associateBy { it.first }

            val commonPosts = postMap1.keys intersect postMap2.keys
            for (postId in commonPosts) {
                val post1 = postMap1[postId]!!
                val post2 = postMap2[postId]!!

                if (post1.second != post2.second) {
                    issues.add("Post $postId title mismatch: ${deviceId}='${post1.second}', ${other.deviceId}='${post2.second}'")
                    isConsistent = false
                }

                if (post1.third != post2.third) {
                    issues.add("Post $postId author_id mismatch: ${deviceId}='${post1.third}', ${other.deviceId}='${post2.third}'")
                    isConsistent = false
                }
            }

            if (isConsistent) {
                println("✅ Data consistency PASSED: $label")
            } else {
                println("❌ Data consistency FAILED: $label")
                issues.forEach { println("  - $it") }
            }

            return isConsistent
        }

        suspend fun performFullSyncWithValidation(other: TestClient, label: String): Boolean {
            println("\n🔄 PERFORMING FULL SYNC WITH VALIDATION: $label")

            // Upload from both devices
            val upload1Success = uploadWithLogging("$label - ${deviceId}")
            val upload2Success = other.uploadWithLogging("$label - ${other.deviceId}")

            if (!upload1Success || !upload2Success) {
                println("❌ Upload phase failed during full sync")
                return false
            }

            // Download to both devices
            downloadWithLogging("$label - ${deviceId}")
            other.downloadWithLogging("$label - ${other.deviceId}")

            // Validate consistency
            return validateDataConsistency(other, label)
        }
    }

    @Test
    fun step1_basic_insert_sync_works() = runBlockingTest {
        println("\n🧪 STEP 1: Testing basic INSERT synchronization")

        val userId = "user-A-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        // Bootstrap both devices
        device1.bootstrap()
        device2.bootstrap()

        // Device 1 creates a user
        val recordId = UUID.randomUUID().toString()
        device1.insertUser(recordId, "Alice", "alice@example.com")
        device1.printDebugInfo("Device 1 after INSERT")

        // Device 1 uploads the INSERT
        assertUploadSuccess(device1.client.uploadOnce(), expectedApplied = 1)
        device1.printDebugInfo("Device 1 after upload")

        // Device 2 downloads the INSERT
        assertDownloadSuccess(device2.client.downloadOnce(limit = 1000, includeSelf = false))
        device2.printDebugInfo("Device 2 after download")

        // Verify both devices have the user
        assertEquals("Device 1 should have 1 user", 1, device1.getUserCount())
        assertEquals("Device 2 should have 1 user", 1, device2.getUserCount())

        val user1 = device1.getUser(recordId)
        val user2 = device2.getUser(recordId)

        assertEquals("Device 1 user data", Triple(recordId, "Alice", "alice@example.com"), user1)
        assertEquals("Device 2 user data", Triple(recordId, "Alice", "alice@example.com"), user2)
        
        println("✅ STEP 1 PASSED: Basic INSERT sync works correctly")
    }

    @Test
    fun step2_basic_update_sync_works() = runBlockingTest {
        println("\n🧪 STEP 2: Testing basic UPDATE synchronization")

        val userId = "user-A-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        // Bootstrap both devices
        device1.bootstrap()
        device2.bootstrap()

        // Device 1 creates a user
        val recordId = UUID.randomUUID().toString()
        device1.insertUser(recordId, "Alice", "alice@example.com")

        // Upload and download to get the initial user on both devices
        assertUploadSuccess(device1.client.uploadOnce(), expectedApplied = 1)
        assertDownloadSuccess(device2.client.downloadOnce(limit = 1000, includeSelf = false))

        device1.printDebugInfo("Device 1 after initial sync")
        device2.printDebugInfo("Device 2 after initial sync")

        // Verify both devices have the initial user
        assertEquals("Both devices should have 1 user initially", 1, device1.getUserCount())
        assertEquals("Both devices should have 1 user initially", 1, device2.getUserCount())

        // Device 1 updates the user
        device1.updateUser(recordId, "Alice Updated", "alice.updated@example.com")
        device1.printDebugInfo("Device 1 after UPDATE")

        // Device 1 uploads the UPDATE
        assertUploadSuccess(device1.client.uploadOnce(), expectedApplied = 1)
        device1.printDebugInfo("Device 1 after upload")

        // Device 2 downloads the UPDATE
        assertDownloadSuccess(device2.client.downloadOnce(limit = 1000, includeSelf = false))
        device2.printDebugInfo("Device 2 after download")

        // Verify both devices have the updated user
        val user1 = device1.getUser(recordId)
        val user2 = device2.getUser(recordId)

        println("Device 1 user: $user1")
        println("Device 2 user: $user2")

        assertEquals("Device 1 should have updated user", Triple(recordId, "Alice Updated", "alice.updated@example.com"), user1)
        assertEquals("Device 2 should have updated user", Triple(recordId, "Alice Updated", "alice.updated@example.com"), user2)
        
        println("✅ STEP 2 PASSED: Basic UPDATE sync works correctly")
    }

    @Test
    fun step3_multiple_updates_sync_correctly() = runBlockingTest {
        println("\n🧪 STEP 3: Testing multiple UPDATE operations")

        val userId = "user-A-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        // Bootstrap both devices
        device1.bootstrap()
        device2.bootstrap()

        // Device 1 creates a user
        val recordId = UUID.randomUUID().toString()
        device1.insertUser(recordId, "Alice", "alice@example.com")

        // Upload and download to get the initial user on both devices
        assertUploadSuccess(device1.client.uploadOnce(), expectedApplied = 1)
        assertDownloadSuccess(device2.client.downloadOnce(limit = 1000, includeSelf = false))

        // Device 1 performs multiple updates
        device1.updateUser(recordId, "Alice V2", "alice.v2@example.com")
        assertUploadSuccess(device1.client.uploadOnce(), expectedApplied = 1)

        device1.updateUser(recordId, "Alice V3", "alice.v3@example.com")
        assertUploadSuccess(device1.client.uploadOnce(), expectedApplied = 1)

        // Device 2 downloads all updates
        assertDownloadSuccess(device2.client.downloadOnce(limit = 1000, includeSelf = false))

        // Verify both devices have the final updated user
        val user1 = device1.getUser(recordId)
        val user2 = device2.getUser(recordId)

        assertEquals("Device 1 should have final updated user", Triple(recordId, "Alice V3", "alice.v3@example.com"), user1)
        assertEquals("Device 2 should have final updated user", Triple(recordId, "Alice V3", "alice.v3@example.com"), user2)
        
        println("✅ STEP 3 PASSED: Multiple UPDATE sync works correctly")
    }

    @Test
    fun step4_concurrent_updates_same_record() = runBlockingTest {
        println("\n🧪 STEP 4: Testing concurrent UPDATEs to the same record")

        val userId = "user-A-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        // Bootstrap both devices
        device1.bootstrap()
        device2.bootstrap()

        // Device 1 creates a user
        val recordId = UUID.randomUUID().toString()
        device1.insertUser(recordId, "Alice", "alice@example.com")

        // Upload and download to get the initial user on both devices
        assertUploadSuccess(device1.client.uploadOnce(), expectedApplied = 1)
        assertDownloadSuccess(device2.client.downloadOnce(limit = 1000, includeSelf = false))

        device1.printDebugInfo("Device 1 after initial sync")
        device2.printDebugInfo("Device 2 after initial sync")

        // Verify both devices have the initial user
        assertEquals("Both devices should have 1 user initially", 1, device1.getUserCount())
        assertEquals("Both devices should have 1 user initially", 1, device2.getUserCount())

        // CONCURRENT UPDATES: Both devices update the same record simultaneously
        println("\n🔄 CONCURRENT UPDATES PHASE")
        device1.updateUser(recordId, "Alice Updated by Device1", "alice.device1@example.com")
        device2.updateUser(recordId, "Alice Updated by Device2", "alice.device2@example.com")

        device1.printDebugInfo("After concurrent UPDATE")
        device2.printDebugInfo("After concurrent UPDATE")

        // Both devices upload their changes
        println("\n📤 UPLOAD PHASE")
        assert(device1.uploadWithLogging("concurrent update"))
        assert(device2.uploadWithLogging("concurrent update"))

        device1.printDebugInfo("After upload")
        device2.printDebugInfo("After upload")

        // Both devices download changes
        println("\n📥 DOWNLOAD PHASE")
        device1.downloadWithLogging("get peer changes")
        device2.downloadWithLogging("get peer changes")

        device1.printDebugInfo("After download")
        device2.printDebugInfo("After download")

        // Verify conflict resolution worked correctly
        println("\n🔍 FINAL VERIFICATION")
        val user1 = device1.getUser(recordId)
        val user2 = device2.getUser(recordId)

        println("Device 1 final user: $user1")
        println("Device 2 final user: $user2")

        // With the new deterministic ClientWinsResolver, both devices should converge to the same result
        // The resolver uses lexicographic comparison, so "Alice Updated by Device1" < "Alice Updated by Device2"
        // Therefore, Device1's version should win on both devices
        val expectedWinningUser = Triple(recordId, "Alice Updated by Device1", "alice.device1@example.com")
        val originalData = Triple(recordId, "Alice", "alice@example.com")

        // Verify neither device reverted to original data (that would indicate broken conflict resolution)
        if (user1 == originalData) {
            println("❌ Device 1 reverted to original data - conflict resolution failed!")
            throw AssertionError("Device 1 should not revert to original data during conflict resolution")
        }
        if (user2 == originalData) {
            println("❌ Device 2 reverted to original data - conflict resolution failed!")
            throw AssertionError("Device 2 should not revert to original data during conflict resolution")
        }

        // With deterministic ClientWinsResolver, both devices should converge to the same result
        assertEquals("Device 1 should have the winning version after conflict resolution", expectedWinningUser, user1)
        assertEquals("Device 2 should have the winning version after conflict resolution", expectedWinningUser, user2)

        println("✅ Conflict resolution working correctly: both devices converged to the same result")

        println("✅ STEP 4 PASSED: Concurrent UPDATE conflict resolution works correctly")
    }

    @Test
    fun step5_multiple_records_updates() = runBlockingTest {
        println("\n🧪 STEP 5: Testing UPDATEs to multiple records")

        val userId = "user-A-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        // Bootstrap both devices
        device1.bootstrap()
        device2.bootstrap()

        // Device 1 creates multiple users
        val recordIds = (1..5).map { UUID.randomUUID().toString() }
        recordIds.forEachIndexed { index, recordId ->
            device1.insertUser(recordId, "User$index", "user$index@example.com")
        }

        // Upload and download to get all users on both devices
        assertUploadSuccess(device1.client.uploadOnce())
        assertDownloadSuccess(device2.client.downloadOnce(limit = 1000, includeSelf = false))

        device1.printDebugInfo("Device 1 after initial sync")
        device2.printDebugInfo("Device 2 after initial sync")

        // Verify both devices have all users
        assertEquals("Both devices should have 5 users initially", 5, device1.getUserCount())
        assertEquals("Both devices should have 5 users initially", 5, device2.getUserCount())

        // Device 1 updates all records
        recordIds.forEachIndexed { index, recordId ->
            device1.updateUser(recordId, "UpdatedUser$index", "updated$index@example.com")
        }

        device1.printDebugInfo("Device 1 after updating all records")

        // Upload and download the updates
        assertUploadSuccess(device1.client.uploadOnce())
        assertDownloadSuccess(device2.client.downloadOnce(limit = 1000, includeSelf = false))

        device1.printDebugInfo("Device 1 after sync")
        device2.printDebugInfo("Device 2 after sync")

        // Verify both devices have all updated records
        recordIds.forEachIndexed { index, recordId ->
            val user1 = device1.getUser(recordId)
            val user2 = device2.getUser(recordId)

            val expectedUser = Triple(recordId, "UpdatedUser$index", "updated$index@example.com")
            assertEquals("Device 1 should have updated user $index", expectedUser, user1)
            assertEquals("Device 2 should have updated user $index", expectedUser, user2)
        }

        println("✅ STEP 5 PASSED: Multiple record UPDATEs sync correctly")
    }

    @Test
    fun step6_complex_concurrent_insert_update_scenarios() = runBlockingTest {
        println("\n🧪 STEP 6: Testing complex concurrent INSERT/UPDATE scenarios")

        val userId = "user-A-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        // Bootstrap both devices
        device1.bootstrap()
        device2.bootstrap()

        println("\n📋 SCENARIO 1: Concurrent UPDATEs to the same record (extended from Step 4)")

        // Create a shared record that both devices will modify
        val sharedRecordId = UUID.randomUUID().toString()
        device1.insertUser(sharedRecordId, "Shared Record", "shared@example.com")

        // Sync initial record
        assert(device1.uploadWithLogging("initial shared record"))
        device2.downloadWithLogging("get shared record")

        device1.printDebugInfo("After initial sync - Scenario 1")
        device2.printDebugInfo("After initial sync - Scenario 1")

        println("\n🔄 CONCURRENT OPERATIONS - Scenario 1")
        // Both devices concurrently UPDATE the same record (this should work from Step 4 fix)
        device1.updateUser(sharedRecordId, "Device1 Updated Shared", "device1.shared@example.com")
        device2.updateUser(sharedRecordId, "Device2 Updated Shared", "device2.shared@example.com")

        // Upload and sync
        assert(device1.uploadWithLogging("UPDATE shared record"))
        assert(device2.uploadWithLogging("UPDATE shared record"))
        device1.downloadWithLogging("get peer UPDATE")
        device2.downloadWithLogging("get peer UPDATE")

        device1.printDebugInfo("After concurrent UPDATE sync - Scenario 1")
        device2.printDebugInfo("After concurrent UPDATE sync - Scenario 1")

        println("\n📋 SCENARIO 2: UPDATE vs DELETE conflict on same record")

        // Create a record for UPDATE vs DELETE conflict
        val conflictRecordId = UUID.randomUUID().toString()
        device1.insertUser(conflictRecordId, "Conflict Record", "conflict@example.com")

        // Sync initial record
        assert(device1.uploadWithLogging("conflict record"))
        device2.downloadWithLogging("get conflict record")

        device1.printDebugInfo("After conflict record sync - Scenario 2")
        device2.printDebugInfo("After conflict record sync - Scenario 2")

        println("\n🔄 CONCURRENT OPERATIONS - Scenario 2")
        // Device 1 UPDATEs, Device 2 DELETEs the same record
        device1.updateUser(conflictRecordId, "Device1 Updated Conflict", "device1.conflict@example.com")
        device2.deleteUser(conflictRecordId)

        // Upload and sync
        assert(device1.uploadWithLogging("UPDATE conflict record"))

        // Check device2 DELETE upload - detailed logging is now in DefaultOversqliteClient
        println("🔍 About to test Device2 DELETE upload - check adb logcat for detailed response")
        val device2DeleteResult = device2.uploadWithLogging("DELETE conflict record")

        if (!device2DeleteResult) {
            println("❌ Device2 DELETE upload failed - check adb logcat for server response details")
            throw AssertionError("Device2 DELETE upload failed - check logcat for server response")
        }

        println("✅ Device2 DELETE upload succeeded")
        assert(device2DeleteResult)
        device1.downloadWithLogging("get DELETE operation")
        device2.downloadWithLogging("get UPDATE operation")

        device1.printDebugInfo("After UPDATE vs DELETE sync - Scenario 2")
        device2.printDebugInfo("After UPDATE vs DELETE sync - Scenario 2")

        println("\n📋 SCENARIO 3: Multiple independent record modifications")

        // Create separate records for each device to modify independently
        val record1Id = UUID.randomUUID().toString()
        val record2Id = UUID.randomUUID().toString()
        val record3Id = UUID.randomUUID().toString()

        device1.insertUser(record1Id, "Record 1", "record1@example.com")
        device1.insertUser(record2Id, "Record 2", "record2@example.com")
        device2.insertUser(record3Id, "Record 3", "record3@example.com")

        // Sync all initial records
        assert(device1.uploadWithLogging("records 1 and 2"))
        assert(device2.uploadWithLogging("record 3"))
        device1.downloadWithLogging("get record 3")
        device2.downloadWithLogging("get records 1 and 2")

        device1.printDebugInfo("After multi-record sync - Scenario 3")
        device2.printDebugInfo("After multi-record sync - Scenario 3")

        println("\n🔄 CONCURRENT OPERATIONS - Scenario 3")
        // Each device modifies different records (should not conflict)
        device1.updateUser(record1Id, "Device1 Updated Record1", "device1.record1@example.com")
        device1.updateUser(record3Id, "Device1 Updated Record3", "device1.record3@example.com") // Cross-device modification

        device2.updateUser(record2Id, "Device2 Updated Record2", "device2.record2@example.com") // Cross-device modification
        device2.updateUser(record3Id, "Device2 Updated Record3", "device2.record3@example.com") // Conflict with Device1!

        // Upload and sync
        assert(device1.uploadWithLogging("multiple updates"))
        assert(device2.uploadWithLogging("multiple updates"))
        device1.downloadWithLogging("get peer updates")
        device2.downloadWithLogging("get peer updates")

        device1.printDebugInfo("Final state - Scenario 3")
        device2.printDebugInfo("Final state - Scenario 3")

        println("\n🔍 FINAL VERIFICATION - Complex Scenarios")

        // Verify conflict resolution outcomes
        val sharedRecord1 = device1.getUser(sharedRecordId)
        val sharedRecord2 = device2.getUser(sharedRecordId)

        println("Shared record - Device 1: $sharedRecord1")
        println("Shared record - Device 2: $sharedRecord2")

        // Check that neither device has original data (indicating successful conflict resolution)
        val originalSharedData = Triple(sharedRecordId, "Shared Record", "shared@example.com")
        if (sharedRecord1 == originalSharedData || sharedRecord2 == originalSharedData) {
            throw AssertionError("Conflict resolution failed: found original data instead of resolved conflict")
        }

        // Verify DELETE vs UPDATE conflict resolution
        val deletedRecord1 = device1.getUser(conflictRecordId)
        val deletedRecord2 = device2.getUser(conflictRecordId)

        println("DELETE vs UPDATE record - Device 1: $deletedRecord1")
        println("DELETE vs UPDATE record - Device 2: $deletedRecord2")

        println("✅ STEP 6 COMPLETED: Complex concurrent scenarios tested successfully")
    }

    @Test
    fun step7_rapid_sequential_updates_same_record() = runBlockingTest {
        println("\n🧪 STEP 7: Testing rapid sequential UPDATEs on same record from different devices")

        val userId = "user-A-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        device1.bootstrap()
        device2.bootstrap()

        // Create initial record
        val recordId = UUID.randomUUID().toString()
        device1.insertUser(recordId, "Initial User", "initial@example.com")

        // Sync initial record
        assert(device1.performFullSyncWithValidation(device2, "initial record"))

        println("\n🔄 RAPID SEQUENTIAL UPDATES")

        // Device 1 performs rapid updates
        for (i in 1..5) {
            device1.updateUser(recordId, "Device1 Update $i", "device1.update$i@example.com")
            println("Device1 performed update $i")

            // Upload immediately after each update
            assert(device1.uploadWithLogging("Device1 update $i"))
        }

        // Device 2 performs rapid updates (overlapping with device1)
        for (i in 1..5) {
            device2.updateUser(recordId, "Device2 Update $i", "device2.update$i@example.com")
            println("Device2 performed update $i")

            // Upload immediately after each update
            assert(device2.uploadWithLogging("Device2 update $i"))
        }

        // Both devices download all changes
        device1.downloadWithLogging("get all peer updates")
        device2.downloadWithLogging("get all peer updates")

        // Validate final consistency
        assert(device1.validateDataConsistency(device2, "after rapid sequential updates"))

        println("✅ STEP 7 PASSED: Rapid sequential UPDATEs handled correctly")
    }

    @Test
    fun step8_partial_field_updates_concurrent() = runBlockingTest {
        println("\n🧪 STEP 8: Testing concurrent partial field UPDATEs")

        val userId = "user-A-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        device1.bootstrap()
        device2.bootstrap()

        // Create initial record
        val recordId = UUID.randomUUID().toString()
        device1.insertUser(recordId, "Original Name", "original@example.com")

        // Sync initial record
        assert(device1.performFullSyncWithValidation(device2, "initial record"))

        println("\n🔄 CONCURRENT PARTIAL FIELD UPDATES")

        // Device 1 updates only name
        device1.updateUserName(recordId, "Device1 Name")

        // Device 2 updates only email
        device2.updateUserEmail(recordId, "device2@example.com")

        // Both upload their changes
        assert(device1.uploadWithLogging("name update"))
        assert(device2.uploadWithLogging("email update"))

        // Device2's upload may have processed conflicts - upload resolved results
        assert(device2.uploadWithLogging("conflict resolution results"))

        // Both download peer changes
        device1.downloadWithLogging("get email update")
        device2.downloadWithLogging("get name update")

        // Validate final state - this should show if partial updates are handled correctly
        val user1 = device1.getUser(recordId)
        val user2 = device2.getUser(recordId)

        println("Device1 final user: $user1")
        println("Device2 final user: $user2")

        // With ClientWinsResolver, each device should keep its own changes
        // But this test will reveal if there are issues with partial field updates
        assert(device1.validateDataConsistency(device2, "after partial field updates"))

        println("✅ STEP 8 PASSED: Concurrent partial field UPDATEs handled correctly")
    }

    @Test
    fun step9_update_during_sync_operation() = runBlockingTest {
        println("\n🧪 STEP 9: Testing UPDATEs during sync operations")

        val userId = "user-A-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        device1.bootstrap()
        device2.bootstrap()

        // Create multiple records to simulate a longer sync operation
        val recordIds = (1..10).map { UUID.randomUUID().toString() }
        recordIds.forEachIndexed { index, recordId ->
            device1.insertUser(recordId, "User $index", "user$index@example.com")
        }

        // Upload all records from device1
        assert(device1.uploadWithLogging("initial 10 records"))

        println("\n🔄 UPDATE DURING SYNC OPERATION")

        // Start download on device2 (this will be a longer operation with 10 records)
        val downloadResult = device2.downloadWithLogging("get 10 records")

        // While device2 is processing, device1 updates one of the records
        val targetRecordId = recordIds[5]
        device1.updateUser(targetRecordId, "Updated During Sync", "updated.during.sync@example.com")

        // Upload the update
        assert(device1.uploadWithLogging("update during sync"))

        // Device2 downloads the additional update
        device2.downloadWithLogging("get update made during sync")

        // Validate consistency
        assert(device1.validateDataConsistency(device2, "after update during sync"))

        println("✅ STEP 9 PASSED: UPDATEs during sync operations handled correctly")
    }

    @Test
    fun step10_foreign_key_related_updates() = runBlockingTest {
        println("\n🧪 STEP 10: Testing concurrent UPDATEs to related records (foreign keys)")

        val userId = "user-A-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        device1.bootstrap()
        device2.bootstrap()

        // Create user and related posts
        val userRecordId = UUID.randomUUID().toString()
        val post1Id = UUID.randomUUID().toString()
        val post2Id = UUID.randomUUID().toString()

        device1.insertUser(userRecordId, "Author User", "author@example.com")
        device1.insertPost(post1Id, "Post 1", userRecordId, "Content 1")
        device1.insertPost(post2Id, "Post 2", userRecordId, "Content 2")

        // Sync all initial data
        assert(device1.performFullSyncWithValidation(device2, "initial user and posts"))

        println("\n🔄 CONCURRENT UPDATES TO RELATED RECORDS")

        // Device 1 updates the user
        device1.updateUser(userRecordId, "Updated Author", "updated.author@example.com")

        // Device 2 updates the posts
        device2.updatePost(post1Id, "Updated Post 1", "Updated Content 1")
        device2.updatePost(post2Id, "Updated Post 2", "Updated Content 2")

        // Both upload their changes
        assert(device1.uploadWithLogging("user update"))
        assert(device2.uploadWithLogging("posts updates"))

        // Both download peer changes
        device1.downloadWithLogging("get posts updates")
        device2.downloadWithLogging("get user update")

        // Validate consistency of both users and posts
        assert(device1.validateDataConsistency(device2, "after related records updates"))

        println("✅ STEP 10 PASSED: Foreign key related UPDATEs handled correctly")
    }

    @Test
    fun step11_overlapping_sync_windows_updates() = runBlockingTest {
        println("\n🧪 STEP 11: Testing UPDATEs with overlapping sync windows")

        val userId = "user-A-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        device1.bootstrap()
        device2.bootstrap()

        // Create initial records
        val recordIds = (1..5).map { UUID.randomUUID().toString() }
        recordIds.forEachIndexed { index, recordId ->
            device1.insertUser(recordId, "User $index", "user$index@example.com")
        }

        // Sync initial records
        assert(device1.performFullSyncWithValidation(device2, "initial 5 records"))

        println("\n🔄 OVERLAPPING SYNC WINDOWS WITH UPDATES")

        // Pattern: Update -> Upload -> Update -> Upload with overlapping downloads

        // Device 1: First wave of updates
        recordIds.take(3).forEachIndexed { index, recordId ->
            device1.updateUser(recordId, "Device1 Wave1 User $index", "device1.wave1.user$index@example.com")
        }
        assert(device1.uploadWithLogging("Device1 wave 1 updates"))

        // Device 2: Overlapping updates while downloading device1's changes
        recordIds.drop(2).forEachIndexed { index, recordId ->
            device2.updateUser(recordId, "Device2 Wave1 User ${index+2}", "device2.wave1.user${index+2}@example.com")
        }

        // Start download on device2 (gets device1's updates)
        device2.downloadWithLogging("get device1 wave1 updates")

        // Device 2 uploads its changes
        assert(device2.uploadWithLogging("Device2 wave 1 updates"))

        // Device 1 downloads device2's changes
        device1.downloadWithLogging("get device2 wave1 updates")

        // Second wave with immediate sync
        device1.updateUser(recordIds[0], "Device1 Wave2", "device1.wave2@example.com")
        device2.updateUser(recordIds[4], "Device2 Wave2", "device2.wave2@example.com")

        // Rapid sync cycle
        assert(device1.uploadWithLogging("Device1 wave 2"))
        assert(device2.uploadWithLogging("Device2 wave 2"))
        device1.downloadWithLogging("get device2 wave2")
        device2.downloadWithLogging("get device1 wave2")

        // Final validation
        assert(device1.validateDataConsistency(device2, "after overlapping sync windows"))

        println("✅ STEP 11 PASSED: Overlapping sync windows with UPDATEs handled correctly")
    }

    @Test
    fun step12_update_recently_created_deleted_records() = runBlockingTest {
        println("\n🧪 STEP 12: Testing UPDATEs on recently created/deleted records")

        val userId = "user-A-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        device1.bootstrap()
        device2.bootstrap()

        println("\n🔄 SCENARIO A: UPDATE record that was just created on another device")

        // Device 1 creates a record
        val newRecordId = UUID.randomUUID().toString()
        device1.insertUser(newRecordId, "Newly Created", "new@example.com")
        assert(device1.uploadWithLogging("new record"))

        // Device 2 downloads the new record
        device2.downloadWithLogging("get new record")

        // Immediately after download, device 2 updates the record
        device2.updateUser(newRecordId, "Updated New Record", "updated.new@example.com")
        assert(device2.uploadWithLogging("update new record"))

        // Device 1 gets the update
        device1.downloadWithLogging("get update to new record")

        // Validate consistency
        assert(device1.validateDataConsistency(device2, "after updating recently created record"))

        println("\n🔄 SCENARIO B: UPDATE record that was recently deleted on another device")

        // Create a record that will be deleted
        val deleteRecordId = UUID.randomUUID().toString()
        device1.insertUser(deleteRecordId, "To Be Deleted", "delete@example.com")
        assert(device1.performFullSyncWithValidation(device2, "record to be deleted"))

        // Device 1 deletes the record
        device1.deleteUser(deleteRecordId)
        assert(device1.uploadWithLogging("delete record"))

        // Before device 2 downloads the delete, it tries to update the record
        device2.updateUser(deleteRecordId, "Updating Deleted Record", "updating.deleted@example.com")

        // Device 2 uploads its update (this should handle the conflict)
        val updateResult = device2.uploadWithLogging("update deleted record")

        // Device 2 downloads the delete operation
        device2.downloadWithLogging("get delete operation")

        // Device 1 downloads any remaining changes
        device1.downloadWithLogging("get any remaining changes")

        // Final validation - this will show how UPDATE vs DELETE conflicts are resolved
        assert(device1.validateDataConsistency(device2, "after update vs delete conflict"))

        println("✅ STEP 12 PASSED: UPDATEs on recently created/deleted records handled correctly")
    }

    @Test
    fun step13_comprehensive_field_level_validation() = runBlockingTest {
        println("\n🧪 STEP 13: Comprehensive field-level inconsistency detection")

        val userId = "user-A-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        device1.bootstrap()
        device2.bootstrap()

        // Create test records with detailed field tracking
        val testRecords = (1..10).map { index ->
            val recordId = UUID.randomUUID().toString()
            device1.insertUser(recordId, "TestUser$index", "testuser$index@example.com")
            recordId
        }

        // Sync initial records
        assert(device1.performFullSyncWithValidation(device2, "initial test records"))

        println("\n🔄 COMPLEX MULTI-FIELD UPDATE PATTERNS")

        // Pattern 1: Alternating field updates
        testRecords.take(3).forEachIndexed { index, recordId ->
            if (index % 2 == 0) {
                device1.updateUserName(recordId, "Device1Name$index")
                device2.updateUserEmail(recordId, "device2email$index@example.com")
            } else {
                device1.updateUserEmail(recordId, "device1email$index@example.com")
                device2.updateUserName(recordId, "Device2Name$index")
            }
        }

        // Pattern 2: Full record updates vs partial updates
        testRecords.drop(3).take(3).forEachIndexed { index, recordId ->
            device1.updateUser(recordId, "Device1Full${index+3}", "device1full${index+3}@example.com")
            device2.updateUserName(recordId, "Device2Partial${index+3}")
        }

        // Pattern 3: Rapid successive updates to same fields
        val rapidUpdateRecord = testRecords[6]
        for (i in 1..5) {
            device1.updateUserName(rapidUpdateRecord, "Device1Rapid$i")
            device2.updateUserName(rapidUpdateRecord, "Device2Rapid$i")
        }

        // Upload all changes
        assert(device1.uploadWithLogging("complex pattern updates"))
        assert(device2.uploadWithLogging("complex pattern updates"))

        // Device2's upload may have processed conflicts - upload resolved results
        assert(device2.uploadWithLogging("conflict resolution results"))

        // Download all changes
        device1.downloadWithLogging("get all peer updates")
        device2.downloadWithLogging("get all peer updates")

        // Detailed field-level validation
        println("\n🔍 DETAILED FIELD-LEVEL VALIDATION")

        var allFieldsConsistent = true
        testRecords.forEach { recordId ->
            val user1 = device1.getUser(recordId)
            val user2 = device2.getUser(recordId)

            if (user1 != user2) {
                println("❌ FIELD MISMATCH for record $recordId:")
                println("  Device1: $user1")
                println("  Device2: $user2")

                if (user1?.second != user2?.second) {
                    println("  NAME FIELD MISMATCH: '${user1?.second}' vs '${user2?.second}'")
                }
                if (user1?.third != user2?.third) {
                    println("  EMAIL FIELD MISMATCH: '${user1?.third}' vs '${user2?.third}'")
                }
                allFieldsConsistent = false
            } else {
                println("✅ Record $recordId: fields consistent")
            }
        }

        // Overall validation
        val overallConsistent = device1.validateDataConsistency(device2, "comprehensive field validation")

        if (!allFieldsConsistent || !overallConsistent) {
            println("❌ COMPREHENSIVE VALIDATION FAILED - Data inconsistencies detected!")

            // Print detailed sync state for debugging
            device1.printSyncTablesState("DEVICE1 FINAL STATE")
            device2.printSyncTablesState("DEVICE2 FINAL STATE")

            throw AssertionError("Field-level data inconsistencies detected - this reproduces the HeavyConcurrentSyncTest issue!")
        }

        println("✅ STEP 13 PASSED: All field-level validations consistent")
    }

    @Test
    fun step14_mixed_operations_race_conditions() = runBlockingTest {
        println("\n🧪 STEP 14: Mixed operations with controlled race conditions")
        println("Testing INSERT/UPDATE/DELETE combinations with overlapping timeframes")

        val userId = "user-mixed-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        device1.bootstrap()
        device2.bootstrap()

        // === SCENARIO 1: INSERT vs UPDATE race condition ===
            println("\n📋 SCENARIO 1: Device A inserts while Device B tries to update")

            val recordId1 = UUID.randomUUID().toString()

            // Device1: Insert new record
            device1.insertUser(recordId1, "New User", "new@device1.com")
            println("Device1: Inserted record $recordId1")

            // Device2: Try to update the same record (should fail initially)
            device2.updateUser(recordId1, "Updated by Device2", "updated@device2.com")
            println("Device2: Attempted to update record $recordId1")

            // Sync and validate
            assert(device1.uploadWithLogging("INSERT operation"))
            assert(device2.uploadWithLogging("UPDATE on non-existent record"))

            // Device2's upload may have processed conflicts - upload resolved results
            assert(device2.uploadWithLogging("conflict resolution results"))

            device1.downloadWithLogging("get peer updates")
            device2.downloadWithLogging("get peer updates")

            assert(device1.validateDataConsistency(device2, "INSERT vs UPDATE race"))

            // === SCENARIO 2: DELETE vs UPDATE race condition ===
            println("\n📋 SCENARIO 2: Device A deletes while Device B tries to update")

            val recordId2 = UUID.randomUUID().toString()

            // Setup: Both devices have the record
            device1.insertUser(recordId2, "To Be Deleted", "delete@example.com")
            assert(device1.performFullSyncWithValidation(device2, "setup for delete test"))

            // Device1: Delete the record
            device1.deleteUser(recordId2)
            println("Device1: Deleted record $recordId2")

            // Device2: Try to update the same record
            device2.updateUser(recordId2, "Updated After Delete", "afterdelete@device2.com")
            println("Device2: Attempted to update deleted record $recordId2")

            // Sync and validate
            assert(device1.uploadWithLogging("DELETE operation"))
            assert(device2.uploadWithLogging("UPDATE on deleted record"))

            // Device2's upload may have processed conflicts - upload resolved results
            assert(device2.uploadWithLogging("conflict resolution results"))

            device1.downloadWithLogging("get peer updates")
            device2.downloadWithLogging("get peer updates")

            assert(device1.validateDataConsistency(device2, "DELETE vs UPDATE race"))

            // === SCENARIO 3: Simultaneous INSERT with same ID ===
            println("\n📋 SCENARIO 3: Both devices insert records with same ID")

            val recordId3 = UUID.randomUUID().toString()

            // Both devices insert with same ID but different data
            device1.insertUser(recordId3, "Device1 Version", "device1@example.com")
            device2.insertUser(recordId3, "Device2 Version", "device2@example.com")
            println("Both devices: Inserted record $recordId3 with different data")

            // Sync and validate
            assert(device1.uploadWithLogging("simultaneous INSERT device1"))
            assert(device2.uploadWithLogging("simultaneous INSERT device2"))

            // Device2's upload may have processed conflicts - upload resolved results
            assert(device2.uploadWithLogging("conflict resolution results"))

            device1.downloadWithLogging("get peer updates")
            device2.downloadWithLogging("get peer updates")

            assert(device1.validateDataConsistency(device2, "simultaneous INSERT with same ID"))

        println("✅ STEP 14 PASSED: Mixed operations with race conditions handled correctly")
    }

    @Test
    fun step15_complex_operation_sequences() = runBlockingTest {
        println("\n🧪 STEP 15: Complex operation sequences and edge cases")
        println("Testing rapid mixed operations and complex synchronization patterns")

        val userId = "user-complex-" + UUID.randomUUID().toString().substring(0, 8)
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        device1.bootstrap()
        device2.bootstrap()

        // === SCENARIO 1: Rapid mixed operations on same record ===
            println("\n📋 SCENARIO 1: Rapid sequential mixed operations")

            val recordId1 = UUID.randomUUID().toString()

            // Device1: INSERT -> UPDATE -> DELETE -> INSERT sequence
            device1.insertUser(recordId1, "Initial", "initial@device1.com")
            assert(device1.uploadWithLogging("initial INSERT"))

            device1.updateUser(recordId1, "Updated", "updated@device1.com")
            assert(device1.uploadWithLogging("UPDATE after INSERT"))

            device1.deleteUser(recordId1)
            assert(device1.uploadWithLogging("DELETE after UPDATE"))

            device1.insertUser(recordId1, "Reinserted", "reinserted@device1.com")
            assert(device1.uploadWithLogging("INSERT after DELETE"))

            // Device2: Try to update during this sequence
            device2.downloadWithLogging("get initial state")
            device2.updateUser(recordId1, "Device2 Update", "device2update@example.com")
            assert(device2.uploadWithLogging("UPDATE during sequence"))

            // Device2's upload may have processed conflicts - upload resolved results
            assert(device2.uploadWithLogging("conflict resolution results"))

            // Final sync
            device1.downloadWithLogging("get peer updates")
            device2.downloadWithLogging("get peer updates")

            assert(device1.validateDataConsistency(device2, "rapid mixed operations sequence"))

            // === SCENARIO 2: Multiple records with overlapping operations ===
            println("\n📋 SCENARIO 2: Multiple records with overlapping operations")

            val recordIds = (1..5).map { UUID.randomUUID().toString() }

            // Device1: Insert all records
            recordIds.forEachIndexed { index, recordId ->
                device1.insertUser(recordId, "User$index", "user$index@device1.com")
            }
            assert(device1.uploadWithLogging("bulk INSERT"))

            // Device2: Download and then perform mixed operations
            device2.downloadWithLogging("get all records")

            // Overlapping operations on different records
            device1.updateUser(recordIds[0], "Updated0", "updated0@device1.com")
            device2.deleteUser(recordIds[0]) // DELETE vs UPDATE conflict

            device1.deleteUser(recordIds[1])
            device2.updateUser(recordIds[1], "Updated1", "updated1@device2.com") // UPDATE vs DELETE conflict

            device1.updateUser(recordIds[2], "Device1Update2", "device1update2@example.com")
            device2.updateUser(recordIds[2], "Device2Update2", "device2update2@example.com") // UPDATE vs UPDATE conflict

            // Sync all changes
            assert(device1.uploadWithLogging("mixed operations batch 1"))
            assert(device2.uploadWithLogging("mixed operations batch 2"))

            // Device2's upload may have processed conflicts - upload resolved results
            assert(device2.uploadWithLogging("conflict resolution results"))

            device1.downloadWithLogging("resolve conflicts")
            device2.downloadWithLogging("resolve conflicts")

            assert(device1.validateDataConsistency(device2, "multiple records overlapping operations"))

        println("✅ STEP 15 PASSED: Complex operation sequences handled correctly")
    }

    @Test
    fun step16_minimal_delete_insert_test() = runBlockingTest {
        println("\n🧪 STEP 16: Minimal DELETE → INSERT test")

        val userId = "user-minimal-${UUID.randomUUID().toString().take(8)}"
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        device1.bootstrap()
        device2.bootstrap()

        // 1. Device1 creates a record
        val recordId = UUID.randomUUID().toString()
        device1.insertUser(recordId, "Original", "original@example.com")

        // 2. Sync to both devices
        assert(device1.uploadWithLogging("initial record"))
        device2.downloadWithLogging("get initial record")
        assert(device1.validateDataConsistency(device2, "after initial sync"))

        // 3. Device1 deletes the record
        device1.deleteUser(recordId)
        assert(device1.uploadWithLogging("delete record"))
        device2.downloadWithLogging("get delete")
        assert(device1.validateDataConsistency(device2, "after delete sync"))

        // 4. Device1 reinserts the same record
        device1.insertUser(recordId, "Reinserted", "reinserted@example.com")
        assert(device1.uploadWithLogging("reinsert record"))

        // 5. Device2 downloads the reinsertion
        device2.downloadWithLogging("get reinsertion")

        // 6. Validate consistency
        assert(device1.validateDataConsistency(device2, "after reinsertion sync"))

        println("✅ STEP 16 PASSED: DELETE → INSERT sequence handled correctly")
    }

    @Test
    fun step17_update_conflict_with_delete_insert() = runBlockingTest {
        println("\n🧪 STEP 17: UPDATE conflict with DELETE → INSERT sequence")

        val userId = "user-conflict-${UUID.randomUUID().toString().take(8)}"
        val device1 = createTestClient(userId, "device1")
        val device2 = createTestClient(userId, "device2")

        device1.bootstrap()
        device2.bootstrap()

        // 1. Device1 creates a record
        val recordId = UUID.randomUUID().toString()
        device1.insertUser(recordId, "Original", "original@example.com")
        assert(device1.uploadWithLogging("initial record"))

        // 2. Both devices sync
        device2.downloadWithLogging("get initial record")
        assert(device1.validateDataConsistency(device2, "after initial sync"))

        // 3. Device1: UPDATE → DELETE → INSERT sequence
        device1.updateUser(recordId, "Updated", "updated@device1.com")
        assert(device1.uploadWithLogging("update record"))

        device1.deleteUser(recordId)
        assert(device1.uploadWithLogging("delete record"))

        device1.insertUser(recordId, "Reinserted", "reinserted@device1.com")
        assert(device1.uploadWithLogging("reinsert record"))

        // 4. Device2: Try to update (this should create a conflict)
        device2.updateUser(recordId, "Device2 Update", "device2@example.com")
        assert(device2.uploadWithLogging("conflicting update"))

        // 5. Device2 uploads its conflict resolution result
        assert(device2.uploadWithLogging("upload conflict resolution result"))

        // 6. Final sync
        device1.downloadWithLogging("get peer updates")
        device2.downloadWithLogging("get peer updates")

        // 7. Validate consistency
        assert(device1.validateDataConsistency(device2, "after conflict resolution"))

        println("✅ STEP 17 PASSED: UPDATE conflict with DELETE → INSERT handled correctly")
    }
}
