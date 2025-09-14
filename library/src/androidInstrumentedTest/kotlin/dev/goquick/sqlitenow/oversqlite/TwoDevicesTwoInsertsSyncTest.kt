package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class TwoDevicesTwoInsertsSyncTest {

    @Before
    fun setUp() {
        if (skipAllTest) {
            throw UnsupportedOperationException("(TEMPORARY setUp) Not implemented yet")
        }
    }

    @Test
    fun one_user_two_devices_delete_should_not_resurrect() = runBlockingTest {


        // Two device DBs
        val db1 = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        val db2 = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        createBusinessTables(db1)
        createBusinessTables(db2)

        // One user, two different devices
        val userA = "user-A-" + java.util.UUID.randomUUID().toString().substring(0, 8)
        val dev1 = "device-1"
        val dev2 = "device-2"

        val client1 = createSyncTestClient(
            db = db1,
            userSub = userA,
            deviceId = dev1,
            tables = listOf("users", "posts")
        )
        val client2 = createSyncTestClient(
            db = db2,
            userSub = userA,
            deviceId = dev2,
            tables = listOf("users", "posts")
        )

        suspend fun fullSync(client: DefaultOversqliteClient, limit: Int = 500) {
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

        // Bootstrap + initial hydrate on both users
        assert(client1.bootstrap(userA, dev1).isSuccess) { "Bootstrap 1 failed: ${client1.bootstrap(userA, dev1).exceptionOrNull()}" }
        assert(client2.bootstrap(userA, dev2).isSuccess) { "Bootstrap 2 failed: ${client2.bootstrap(userA, dev2).exceptionOrNull()}" }
        val hydrate1 = client1.hydrate(includeSelf = false, limit = 1000, windowed = true)
        assert(hydrate1.isSuccess) { "Hydrate 1 failed: ${hydrate1.exceptionOrNull()}" }
        val hydrate2 = client2.hydrate(includeSelf = false, limit = 1000, windowed = true)
        assert(hydrate2.isSuccess) { "Hydrate 2 failed: ${hydrate2.exceptionOrNull()}" }

        // Insert one row per device
        val a1 = java.util.UUID.randomUUID().toString()
        db1.execSQL("INSERT INTO users(id, name, email) VALUES('$a1','Alice','alice@example.com')")
        assertUploadSuccess(client1.uploadOnce(), expectedApplied = 1)

        val b1 = java.util.UUID.randomUUID().toString()
        db2.execSQL("INSERT INTO users(id, name, email) VALUES('$b1','Bob','bob@example.com')")
        assertUploadSuccess(client2.uploadOnce(), expectedApplied = 1)

        // Sync each user once
        assertDownloadSuccess(client1.downloadOnce(limit = 1000, includeSelf = false))
        assertDownloadSuccess(client2.downloadOnce(limit = 1000, includeSelf = false))

        // Now delete A's record locally and upload
        db1.execSQL("DELETE FROM users WHERE id='$a1'")
        assertUploadSuccess(client1.uploadOnce(), expectedApplied = 1)

        // Full-sync flows like the app: upload then page downloads until drained
        fullSync(client1)
        fullSync(client2)
        fullSync(client1)

        // Validate: device 1 should not have resurrected row a1, and should still have one row (b1)
        assertEquals(0, count(db1, "users", "id='$a1'"))
        assertEquals(1, count(db1, "users", "1=1"))

        // Sanity: device 2 shouldn't be affected and still has its own row, and no a1
        fullSync(client2)
        assertEquals(1, count(db2, "users", "id='$b1'"))
        assertEquals(0, count(db2, "users", "id='$a1'"))
    }

    @Test
    fun one_user_two_devices_delete_then_download_before_upload_should_not_resurrect() =
        runBlockingTest {


            val db1 = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
            val db2 = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
            createBusinessTables(db1)
            createBusinessTables(db2)

            val userA = "user-A-" + java.util.UUID.randomUUID().toString().substring(0, 8)
            val dev1 = "device-1"
            val dev2 = "device-2"

            val client1 = createSyncTestClient(
                db = db1,
                userSub = userA,
                deviceId = dev1,
                tables = listOf("users", "posts")
            )
            val client2 = createSyncTestClient(
                db = db2,
                userSub = userA,
                deviceId = dev2,
                tables = listOf("users", "posts")
            )

            suspend fun downloadDrain(client: DefaultOversqliteClient, limit: Int = 500) {
                var more = true
                while (more) {
                    val downloadResult = client.downloadOnce(limit = limit, includeSelf = false)
                    assertDownloadSuccess(downloadResult)
                    val (applied, _) = downloadResult.getOrNull() ?: (0 to 0L)
                    more = applied == limit
                    if (applied == 0) break
                }
            }

            // Bootstrap + initial hydrate on both devices
            assert(client1.bootstrap(userA, dev1).isSuccess)
            assert(client2.bootstrap(userA, dev2).isSuccess)
            assert(client1.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess)
            assert(client2.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess)

            // Insert a1 on device 1 and b1 on device 2
            val a1 = java.util.UUID.randomUUID().toString()
            val b1 = java.util.UUID.randomUUID().toString()
            db1.execSQL("INSERT INTO users(id, name, email) VALUES('$a1','Alice','alice@example.com')")
            assertUploadSuccess(client1.uploadOnce(), expectedApplied = 1)
            db2.execSQL("INSERT INTO users(id, name, email) VALUES('$b1','Bob','bob@example.com')")
            assertUploadSuccess(client2.uploadOnce(), expectedApplied = 1)
            downloadDrain(client1); downloadDrain(client2)

            // Delete a1 on device 1, then mimic download-before-upload
            db1.execSQL("DELETE FROM users WHERE id='$a1'")
            downloadDrain(client1) // app might call download loop first
            // Now upload; internal lookback could re-download a1, but deletion-aware phase should restore it
            assertUploadSuccess(client1.uploadOnce(), expectedApplied = 1)
            downloadDrain(client1)

            // Validate: a1 is not present; b1 remains
            assertEquals(0, count(db1, "users", "id='$a1'"))
            assertEquals(1, count(db1, "users", "id='$b1'"))
        }

    @Test
    fun one_user_two_devices_delete_resurrection_bug() = runBlockingTest {


        val db1 = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        val db2 = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        createBusinessTables(db1)
        createBusinessTables(db2)

        val userA = "user-A-" + java.util.UUID.randomUUID().toString().substring(0, 8)
        val dev1 = "device-1"
        val dev2 = "device-2"

        val client1 = createSyncTestClient(
            db = db1,
            userSub = userA,
            deviceId = dev1,
            tables = listOf("users", "posts")
        )
        val client2 = createSyncTestClient(
            db = db2,
            userSub = userA,
            deviceId = dev2,
            tables = listOf("users", "posts")
        )

        suspend fun fullSync(client: DefaultOversqliteClient) {
            assertUploadSuccessWithConflicts(client.uploadOnce())
            val limit = 500
            var more = true
            while (more) {
                val downloadResult = client.downloadOnce(limit = limit, includeSelf = false)
                assertDownloadSuccess(downloadResult)
                val (applied, _) = downloadResult.getOrNull() ?: (0 to 0L)
                more = applied == limit
                if (applied == 0) break
            }
        }

        // Bootstrap + initial hydrate on both devices
        assert(client1.bootstrap(userA, dev1).isSuccess)
        assert(client2.bootstrap(userA, dev2).isSuccess)
        assert(client1.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess)
        assert(client2.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess)

        // Device 1: Add Person (Record #1)
        val record1 = java.util.UUID.randomUUID().toString()
        db1.execSQL("INSERT INTO users(id, name, email) VALUES('$record1','Alice','alice@example.com')")
        fullSync(client1)
        assertEquals(1, count(db1, "users", "1=1"))

        // Device 2: Add Person (Record #2) - should see both records
        val record2 = java.util.UUID.randomUUID().toString()
        db2.execSQL("INSERT INTO users(id, name, email) VALUES('$record2','Bob','bob@example.com')")
        fullSync(client2)
        assertEquals(2, count(db2, "users", "1=1"))
        assertEquals(1, count(db2, "users", "id='$record1'"))
        assertEquals(1, count(db2, "users", "id='$record2'"))

        // Device 2: Delete Record #1 (created by device 1)
        db2.execSQL("DELETE FROM users WHERE id='$record1'")

        // Check _sync_pending before upload
        val pendingBefore =
            db2.prepare("SELECT op, base_version FROM _sync_pending WHERE table_name='users' AND pk_uuid='$record1'")
                .use { st ->
                    if (st.step()) "${st.getText(0)} v${st.getLong(1)}" else "none"
                }
        println("DEBUG: _sync_pending before upload: $pendingBefore")

        fullSync(client2)

        // Check _sync_pending after sync
        val pendingAfter =
            db2.prepare("SELECT op, base_version FROM _sync_pending WHERE table_name='users' AND pk_uuid='$record1'")
                .use { st ->
                    if (st.step()) "${st.getText(0)} v${st.getLong(1)}" else "none"
                }
        println("DEBUG: _sync_pending after sync: $pendingAfter")

        // Check _sync_row_meta
        val metaInfo =
            db2.prepare("SELECT server_version, deleted FROM _sync_row_meta WHERE table_name='users' AND pk_uuid='$record1'")
                .use { st ->
                    if (st.step()) "v${st.getLong(0)} deleted=${st.getLong(1) == 1L}" else "none"
                }
        println("DEBUG: _sync_row_meta: $metaInfo")

        // BUG: Record #1 shows up again on device 2 after sync
        val countAfterDelete = count(db2, "users", "id='$record1'")
        println("DEBUG: Record count after delete+sync: $countAfterDelete")
        assertEquals("Record should stay deleted on device 2", 0, countAfterDelete)
        assertEquals("Device 2 should only have record 2", 1, count(db2, "users", "1=1"))

        // Device 1: Sync - should see correct list (Record 1 gone, only Record 2)
        fullSync(client1)
        assertEquals(
            "Device 1 should not have record 1 after sync",
            0,
            count(db1, "users", "id='$record1'")
        )
        assertEquals("Device 1 should have record 2", 1, count(db1, "users", "id='$record2'"))
        assertEquals("Device 1 should have 1 total record", 1, count(db1, "users", "1=1"))
    }

    @Test
    fun one_user_two_devices_update_lost_bug() = runBlockingTest {


        val db1 = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        val db2 = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        createBusinessTables(db1)
        createBusinessTables(db2)

        val userA = "user-A-" + java.util.UUID.randomUUID().toString().substring(0, 8)
        val dev1 = "device-1"
        val dev2 = "device-2"

        val client1 = createSyncTestClient(
            db = db1,
            userSub = userA,
            deviceId = dev1,
            tables = listOf("users")
        )
        val client2 = createSyncTestClient(
            db = db2,
            userSub = userA,
            deviceId = dev2,
            tables = listOf("users")
        )

        suspend fun fullSync(client: DefaultOversqliteClient) {
            assertUploadSuccessWithConflicts(client.uploadOnce())
            val limit = 500
            var more = true
            while (more) {
                val downloadResult = client.downloadOnce(limit = limit, includeSelf = false)
                assertDownloadSuccess(downloadResult)
                val (applied, _) = downloadResult.getOrNull() ?: (0 to 0L)
                more = applied == limit
                if (applied == 0) break
            }
        }

        // Bootstrap + initial hydrate on both devices
        assert(client1.bootstrap(userA, dev1).isSuccess)
        assert(client2.bootstrap(userA, dev2).isSuccess)
        assert(client1.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess)
        assert(client2.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess)

        // Device 1: Add Person #1
        val record1 = java.util.UUID.randomUUID().toString()
        db1.execSQL("INSERT INTO users(id, name, email) VALUES('$record1','Alice','alice@example.com')")
        fullSync(client1)
        assertEquals(1, count(db1, "users", "1=1"))

        // Device 2: Sync - should see Person #1
        fullSync(client2)
        assertEquals(1, count(db2, "users", "1=1"))
        assertEquals(1, count(db2, "users", "id='$record1'"))

        // Verify initial values on device 2
        val initialName = db2.prepare("SELECT name FROM users WHERE id=?").use { st ->
            st.bindText(1, record1)
            if (st.step()) st.getText(0) else null
        }
        val initialEmail = db2.prepare("SELECT email FROM users WHERE id=?").use { st ->
            st.bindText(1, record1)
            if (st.step()) st.getText(0) else null
        }
        assertEquals("Alice", initialName)
        assertEquals("alice@example.com", initialEmail)

        // Device 2: Update Person #1 (change name and email)
        db2.execSQL("UPDATE users SET name='Alice Updated', email='alice.updated@example.com' WHERE id='$record1'")

        // Check _sync_pending before upload
        val pendingBefore =
            db2.prepare("SELECT op, base_version FROM _sync_pending WHERE table_name='users' AND pk_uuid='$record1'")
                .use { st ->
                    if (st.step()) "${st.getText(0)} v${st.getLong(1)}" else "none"
                }
        println("DEBUG: _sync_pending before upload: $pendingBefore")

        fullSync(client2)

        // Check _sync_pending after sync
        val pendingAfter =
            db2.prepare("SELECT op, base_version FROM _sync_pending WHERE table_name='users' AND pk_uuid='$record1'")
                .use { st ->
                    if (st.step()) "${st.getText(0)} v${st.getLong(1)}" else "none"
                }
        println("DEBUG: _sync_pending after sync: $pendingAfter")

        // Check _sync_row_meta
        val metaInfo =
            db2.prepare("SELECT server_version, deleted FROM _sync_row_meta WHERE table_name='users' AND pk_uuid='$record1'")
                .use { st ->
                    if (st.step()) "v${st.getLong(0)} deleted=${st.getLong(1) == 1L}" else "none"
                }
        println("DEBUG: _sync_row_meta: $metaInfo")

        // Verify update was applied on device 2
        val updatedName = db2.prepare("SELECT name FROM users WHERE id=?").use { st ->
            st.bindText(1, record1)
            if (st.step()) st.getText(0) else null
        }
        val updatedEmail = db2.prepare("SELECT email FROM users WHERE id=?").use { st ->
            st.bindText(1, record1)
            if (st.step()) st.getText(0) else null
        }
        println("DEBUG: After update+sync - name='$updatedName', email='$updatedEmail'")

        // BUG: Update should persist on device 2, not revert to original
        assertEquals("Update should persist on device 2", "Alice Updated", updatedName)
        assertEquals("Update should persist on device 2", "alice.updated@example.com", updatedEmail)

        // Device 1: Sync - should receive the updated Person #1
        fullSync(client1)
        val finalNameDev1 = db1.prepare("SELECT name FROM users WHERE id=?").use { st ->
            st.bindText(1, record1)
            if (st.step()) st.getText(0) else null
        }
        val finalEmailDev1 = db1.prepare("SELECT email FROM users WHERE id=?").use { st ->
            st.bindText(1, record1)
            if (st.step()) st.getText(0) else null
        }

        assertEquals("Device 1 should receive updated name", "Alice Updated", finalNameDev1)
        assertEquals(
            "Device 1 should receive updated email",
            "alice.updated@example.com",
            finalEmailDev1
        )
    }


    @Test
    fun two_devices_each_insert_then_sync_end_up_with_two_users_on_both() = runBlockingTest {


        // Two device DBs
        val db1 = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        val db2 = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))
        createBusinessTables(db1)
        createBusinessTables(db2)

        // Shared user and two different devices
        val userSub = "user-sync-" + java.util.UUID.randomUUID().toString().substring(0, 8)
        val dev1 = "device-1"
        val dev2 = "device-2"

        val client1 = createSyncTestClient(
            db = db1,
            userSub = userSub,
            deviceId = dev1,
            tables = listOf("users", "posts")
        )
        val client2 = createSyncTestClient(
            db = db2,
            userSub = userSub,
            deviceId = dev2,
            tables = listOf("users", "posts")
        )

        // Bootstrap + initial hydrate on both devices
        assert(client1.bootstrap(userSub, dev1).isSuccess) { "Bootstrap 1 failed" }
        assert(client2.bootstrap(userSub, dev2).isSuccess) { "Bootstrap 2 failed" }
        assert(client1.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess)
        assert(client2.hydrate(includeSelf = false, limit = 1000, windowed = true).isSuccess)

        // Insert on device 1 and upload
        val id1 = java.util.UUID.randomUUID().toString()
        db1.execSQL("INSERT INTO users(id, name, email) VALUES('$id1','Alice','alice@example.com')")
        assertUploadSuccess(client1.uploadOnce(), expectedApplied = 1)

        // Insert on device 2 and upload
        val id2 = java.util.UUID.randomUUID().toString()
        db2.execSQL("INSERT INTO users(id, name, email) VALUES('$id2','Bob','bob@example.com')")
        assertUploadSuccess(client2.uploadOnce(), expectedApplied = 1)

        // Sync/download both devices with retries to account for eventual consistency
        suspend fun syncUntilBothSeeTwo(maxAttempts: Int = 10): Pair<Int, Int> {
            var c1 = count(db1, "users", "1=1")
            var c2 = count(db2, "users", "1=1")
            var attempts = 0
            while ((c1 < 2 || c2 < 2) && attempts < maxAttempts) {
                assertDownloadSuccess(client1.downloadOnce(limit = 1000, includeSelf = false))
                assertDownloadSuccess(client2.downloadOnce(limit = 1000, includeSelf = false))
                c1 = count(db1, "users", "1=1")
                c2 = count(db2, "users", "1=1")
                if (c1 < 2 || c2 < 2) kotlinx.coroutines.delay(200)
                attempts++
            }
            return c1 to c2
        }

        val (finalC1, finalC2) = syncUntilBothSeeTwo(maxAttempts = 50)

        // Verify both devices see both records
        assertEquals("Device 1 users count", 2, finalC1)
        assertEquals("Device 2 users count", 2, finalC2)
    }
}

