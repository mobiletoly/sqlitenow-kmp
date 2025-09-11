package dev.goquick.sqlitenow.oversqlite

import android.content.Context
import androidx.sqlite.SQLiteConnection
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.sqlite.execSQL
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class AndroidOversqliteClientTest {

    private lateinit var context: Context
    private lateinit var db: SQLiteConnection

    @Before
    fun setUp() {
        if (skipAllTest) {
            throw UnsupportedOperationException("(TEMPORARY setUp) Not implemented yet")
        }

        context = ApplicationProvider.getApplicationContext()
        db = BundledSQLiteDriver().open(":memory:")
    }

    @After
    fun tearDown() {
        // no-op for bundled driver
    }

    @Test
    fun bootstrap_creates_metadata_and_triggers_and_tracks_changes() = runBlockingTest {
        val db = SafeSQLiteConnection(this.db)
        // Create business tables
        createBusinessTables(db)

        val userSub = "user-kmp-" + java.util.UUID.randomUUID().toString().substring(0, 8)
        val deviceId = "device-kmp"
        val client = createSyncTestClient(
            db = db,
            userSub = userSub,
            deviceId = deviceId,
            tables = listOf("users", "posts")
        )
        assert(
            client.bootstrap(
                userId = userSub,
                sourceId = deviceId
            ).isSuccess
        ) { "Bootstrap failed" }

        // Verify metadata tables
        assertEquals(1, count(db, "sqlite_master", "type='table' AND name='_sync_client_info'"))
        assertEquals(1, count(db, "sqlite_master", "type='table' AND name='_sync_row_meta'"))
        assertEquals(1, count(db, "sqlite_master", "type='table' AND name='_sync_pending'"))

        // Insert into users (should create pending INSERT)
        val newUserId = java.util.UUID.randomUUID().toString()
        db.execSQL("INSERT INTO users(id, name, email) VALUES('" + newUserId + "','John','john@example.com')")
        assertEquals(
            1,
            scalarLong(db, "SELECT COUNT(*) FROM _sync_pending WHERE table_name='users'")
        )
        assertEquals(
            1,
            scalarLong(db, "SELECT COUNT(*) FROM _sync_row_meta WHERE table_name='users'")
        )

        // Update users (should coalesce, still one pending INSERT, payload updated)
        db.execSQL("UPDATE users SET name='John Doe' WHERE id='" + newUserId + "'")
        assertEquals(
            1,
            scalarLong(db, "SELECT COUNT(*) FROM _sync_pending WHERE table_name='users'")
        )

        // Perform upload to server and assert success
        val uploadResult = client.uploadOnce()
        assert(uploadResult.isSuccess) { "Upload failed: ${uploadResult.exceptionOrNull()}" }

        // Optional: Perform a download cycle to ensure no pending server changes
        val page = client.downloadOnce(limit = 1000, includeSelf = false)
        assert(page.isSuccess) { "Download failed: ${page.exceptionOrNull()}" }

        // After successful upload pending should be cleared and meta version > 0
        assertEquals(
            0,
            scalarLong(db, "SELECT COUNT(*) FROM _sync_pending WHERE table_name='users'")
        )
        val sv = scalarLong(
            db,
            "SELECT server_version FROM _sync_row_meta WHERE table_name='users' AND pk_uuid='" + newUserId + "'"
        )
        assert(sv > 0) { "server_version should be > 0 after upload" }
    }

    @Test
    fun hydration_and_multi_sync_cycles_across_two_devices() = runBlockingTest {
        // Device A DB
        val dbA = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        // Device B DB
        val dbB = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))

        // Create business schema in both
        createBusinessTables(dbA)
        createBusinessTables(dbB)


        val userSub = "user-two-dev-" + java.util.UUID.randomUUID().toString().substring(0, 8)
        val devA = "device-A"
        val devB = "device-B"

        val clientA = createSyncTestClient(
            db = dbA,
            userSub = userSub,
            deviceId = devA,
            tables = listOf("users", "posts")
        )
        val clientB = createSyncTestClient(
            db = dbB,
            userSub = userSub,
            deviceId = devB,
            tables = listOf("users", "posts")
        )

        // Bootstrap both
        assert(clientA.bootstrap(userSub, devA).isSuccess) { "Bootstrap A failed" }
        assert(clientB.bootstrap(userSub, devB).isSuccess) { "Bootstrap B failed" }

        // Device A creates data
        val u1 = java.util.UUID.randomUUID().toString()
        val u2 = java.util.UUID.randomUUID().toString()
        dbA.execSQL("INSERT INTO users(id, name, email) VALUES('$u1','Alice','alice@example.com')")
        dbA.execSQL("INSERT INTO users(id, name, email) VALUES('$u2','Bob','bob@example.com')")
        val p1 = java.util.UUID.randomUUID().toString()
        dbA.execSQL("INSERT INTO posts(id, title, content, author_id) VALUES('$p1','Hello','World','$u1')")

        // Upload from device A
        val uploadResult = clientA.uploadOnce()
        assert(uploadResult.isSuccess) { "Upload failed: ${uploadResult.exceptionOrNull()}" }

        // Device B hydrates via API (windowed snapshot, includeSelf=false)
        val hydB = clientB.hydrate(includeSelf = false, limit = 1000, windowed = true)
        assert(hydB.isSuccess) { "Hydration failed: ${hydB.exceptionOrNull()}" }
        // Verify B has the rows
        assertEquals(2, count(dbB, "users", "1=1"))
        assertEquals(1, count(dbB, "posts", "1=1"))
        // Sanity: triggers installed and apply_mode reset
        val trgCount = count(dbB, "sqlite_master", "type='trigger' AND tbl_name='users'")
        assert(trgCount >= 3) { "Expected triggers for users table on B" }
        val amB = scalarLong(dbB, "SELECT apply_mode FROM _sync_client_info LIMIT 1")
        assertEquals(0, amB.toInt())

        // Device B updates Alice
        val u1ExistsOnB = scalarLong(dbB, "SELECT COUNT(*) FROM users WHERE id='$u1'")
        assertEquals(1, u1ExistsOnB.toInt())
        val beforeNameB = scalarText(dbB, "SELECT name FROM users WHERE id='$u1'")
        // Update to a unique value to avoid no-op updates if server already had same value from previous runs
        val newName = "Alice_" + java.util.UUID.randomUUID().toString().substring(0, 8)
        dbB.execSQL("UPDATE users SET name='" + newName + "' WHERE id='" + u1 + "'")
        val afterNameB = scalarText(dbB, "SELECT name FROM users WHERE id='$u1'")
        assert(afterNameB == newName) { "Local update on B did not apply" }
        // Ensure B has queued a pending update
        val pendingB = scalarLong(dbB, "SELECT COUNT(*) FROM _sync_pending")
        assert(pendingB > 0) { "No pending changes queued on device B after UPDATE" }
        assert(clientB.uploadOnce().isSuccess)

        // Device A downloads (no direct cursor manipulation). Retry until we see the update.
        var seen = false
        repeat(15) {
            clientA.downloadOnce(limit = 1000, includeSelf = false)
            val n = scalarText(dbA, "SELECT name FROM users WHERE id='$u1'")
            if (n == newName) {
                seen = true; return@repeat
            }
            kotlinx.coroutines.delay(200)
        }
        assert(seen) { "Alice name not updated on A" }

        // Device A deletes Bob (retry upload up to 3 times to avoid transient failures)
        dbA.execSQL("DELETE FROM users WHERE id='$u2'")
        // Ensure A has queued a pending DELETE
        val pendingAAfterDelete = scalarLong(
            dbA,
            "SELECT COUNT(*) FROM _sync_pending WHERE table_name='users' AND pk_uuid='$u2'"
        )
        assert(pendingAAfterDelete > 0) { "No pending DELETE on device A" }
        run {
            var ok = false
            repeat(3) {
                if (clientA.uploadOnce().isSuccess) {
                    ok = true; return@repeat
                }
            }
            assert(ok) { "Delete upload failed after retries" }
        }

        // Device B downloads and verifies Bob gone
        val dlB = clientB.downloadOnce(limit = 1000, includeSelf = false)
        assert(dlB.isSuccess)
        assertEquals(1, count(dbB, "users", "1=1"))
    }

    @Test
    fun realistic_uninstall_and_hydrate_multiple_cycles() = runBlockingTest {
        // Helper to create business schema


        // Device A (initial install)
        val dbA = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        createBusinessTables(dbA)
        val userSub = "user-real-" + java.util.UUID.randomUUID().toString().substring(0, 8)
        val devA = "device-A-real"
        val clientA = createSyncTestClient(
            db = dbA,
            userSub = userSub,
            deviceId = devA,
            tables = listOf("users", "posts")
        )
        assert(clientA.bootstrap(userSub, devA).isSuccess) { "Bootstrap A failed" }

        // Generate batch1 data
        val userIds1 = mutableListOf<String>()
        repeat(200) {
            val id = java.util.UUID.randomUUID().toString()
            userIds1 += id
            dbA.execSQL("INSERT INTO users(id, name, email) VALUES('$id','User$it','u$it@example.com')")
            if ((it + 1) % 50 == 0) {
                assert(clientA.uploadOnce().isSuccess)
            }
        }
        // Posts batch 1
        repeat(400) {
            val id = java.util.UUID.randomUUID().toString()
            val author = userIds1[it % userIds1.size]
            dbA.execSQL("INSERT INTO posts(id, title, content, author_id) VALUES('$id','T$it','C$it','$author')")
            if ((it + 1) % 100 == 0) {
                val uploadResult = clientA.uploadOnce()
                assert(uploadResult.isSuccess) { "Upload failed at iteration $it: ${uploadResult.exceptionOrNull()}" }
            }
        }
        // Final upload drain
        repeat(3) { clientA.uploadOnce() }
        assertEquals(0, count(dbA, "_sync_pending", "1=1"))

        // Simulate uninstall -> New device B with fresh DB
        val dbB = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        createBusinessTables(dbB)
        val devB = "device-B-real"
        val clientB = createSyncTestClient(
            db = dbB,
            userSub = userSub,
            deviceId = devB,
            tables = listOf("users", "posts")
        )
        assert(clientB.bootstrap(userSub, devB).isSuccess) { "Bootstrap B failed" }
        val hyd1 = clientB.hydrate(includeSelf = false, limit = 500, windowed = true)
        assert(hyd1.isSuccess) { "Hydrate #1 failed: ${hyd1.exceptionOrNull()}" }
        assertEquals(200, count(dbB, "users", "1=1"))
        assertEquals(400, count(dbB, "posts", "1=1"))

        // Add more data on device B (batch2)
        val userIds2 = mutableListOf<String>()
        repeat(150) {
            val id = java.util.UUID.randomUUID().toString()
            userIds2 += id
            dbB.execSQL("INSERT INTO users(id, name, email) VALUES('$id','User2_$it','u2_$it@example.com')")
            if ((it + 1) % 50 == 0) {
                assert(clientB.uploadOnce().isSuccess)
            }
        }
        repeat(300) {
            val id = java.util.UUID.randomUUID().toString()
            val allUsers = userIds1 + userIds2
            val author = allUsers[it % allUsers.size]
            dbB.execSQL("INSERT INTO posts(id, title, content, author_id) VALUES('$id','T2_$it','C2_$it','$author')")
            if ((it + 1) % 100 == 0) {
                assert(clientB.uploadOnce().isSuccess)
            }
        }
        repeat(3) { clientB.uploadOnce() }
        assertEquals(0, count(dbB, "_sync_pending", "1=1"))

        // Simulate uninstall again -> New device C
        val dbC = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        createBusinessTables(dbC)
        val devC = "device-C-real"
        val clientC = createSyncTestClient(
            db = dbC,
            userSub = userSub,
            deviceId = devC,
            tables = listOf("users", "posts")
        )
        assert(clientC.bootstrap(userSub, devC).isSuccess) { "Bootstrap C failed" }
        val hyd2 = clientC.hydrate(includeSelf = false, limit = 500, windowed = true)
        assert(hyd2.isSuccess) { "Hydrate #2 failed: ${hyd2.exceptionOrNull()}" }
        // Expect totals: 200+150 users, 400+300 posts
        assertEquals(350, count(dbC, "users", "1=1"))
        assertEquals(700, count(dbC, "posts", "1=1"))
    }
}
