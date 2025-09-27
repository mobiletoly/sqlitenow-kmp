package dev.goquick.sqlitenow.oversqlite

import android.content.Context
import androidx.sqlite.SQLiteConnection
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Integration test to verify that sync works with custom primary key column names
 * instead of the default "id" column.
 */
@RunWith(AndroidJUnit4::class)
class CustomPrimaryKeyTest {

    private lateinit var context: Context
    private lateinit var rawDb: SQLiteConnection
    private lateinit var db: SafeSQLiteConnection
    private lateinit var client: DefaultOversqliteClient

    @Before
    fun setUp() {
        if (skipAllOversqliteTest) {
            throw UnsupportedOperationException("(TEMPORARY setUp) Not implemented yet")
        }

        context = ApplicationProvider.getApplicationContext()
        rawDb = BundledSQLiteDriver().open(":memory:")
        db = SafeSQLiteConnection(rawDb)
        runBlockingTest {
            setupTestTables()
        }
    }

    @After
    fun tearDown() {
        runBlockingTest {
            if (::client.isInitialized) {
                client.close()
            }
        }
        // no-op for bundled driver
    }

    private suspend fun setupTestTables() {
        // Create a table with custom primary key "uuid" instead of "id"
        db.execSQL("""
            CREATE TABLE custom_users (
                uuid TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """.trimIndent())

        // Create a table with custom primary key "code" instead of "id"
        db.execSQL("""
            CREATE TABLE products (
                code TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                category TEXT
            )
        """.trimIndent())

        // Create a table with traditional "id" primary key for comparison
        db.execSQL("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_uuid TEXT NOT NULL,
                product_code TEXT NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                total REAL NOT NULL
            )
        """.trimIndent())
    }

    @Test
    fun testCustomPrimaryKeyDetection() = runBlockingTest {
        // Create sync client with custom primary key tables
        val syncTables = listOf(
            SyncTable(tableName = "custom_users", syncKeyColumnName = "uuid"),
            SyncTable(tableName = "products", syncKeyColumnName = "code"),
            SyncTable(tableName = "orders") // Uses default "id"
        )

        client = createSyncTestClientWithCustomPK(
            db = db,
            userSub = "test-user",
            deviceId = "test-device",
            syncTables = syncTables
        )

        // Bootstrap should succeed and create triggers with correct primary key columns
        val bootstrapResult = client.bootstrap(userId = "test-user", sourceId = "test-device")
        assertTrue("Bootstrap should succeed with custom primary keys", bootstrapResult.isSuccess)

        // Verify triggers were created correctly by checking trigger SQL
        verifyTriggersCreated()
    }

    @Test
    fun testSyncWithCustomPrimaryKeys() = runBlockingTest {
        // Setup client with custom primary keys
        val syncTables = listOf(
            SyncTable(tableName = "custom_users", syncKeyColumnName = "uuid"),
            SyncTable(tableName = "products", syncKeyColumnName = "code"),
            SyncTable(tableName = "orders") // Uses default "id"
        )

        client = createSyncTestClientWithCustomPK(
            db = db,
            userSub = "test-user",
            deviceId = "test-device",
            syncTables = syncTables
        )

        // Bootstrap
        client.bootstrap(userId = "test-user", sourceId = "test-device").getOrThrow()

        // Insert data with custom primary keys
        db.execSQL("""
            INSERT INTO custom_users (uuid, name, email) 
            VALUES ('user-123', 'John Doe', 'john@example.com')
        """.trimIndent())

        db.execSQL("""
            INSERT INTO products (code, name, price, category) 
            VALUES ('PROD-001', 'Widget', 19.99, 'Electronics')
        """.trimIndent())

        db.execSQL("""
            INSERT INTO orders (user_uuid, product_code, quantity, total) 
            VALUES ('user-123', 'PROD-001', 2, 39.98)
        """.trimIndent())

        // Verify that sync metadata was created with correct primary key values
        verifyPendingChanges()
        verifyRowMetadata()
    }

    @Test
    fun testAutoDetectPrimaryKey() = runBlockingTest {
        // Create sync client with null syncKeyColumnName to test auto-detection
        val syncTables = listOf(
            SyncTable(tableName = "custom_users", syncKeyColumnName = null), // Should auto-detect "uuid"
            SyncTable(tableName = "products", syncKeyColumnName = null), // Should auto-detect "code"
            SyncTable(tableName = "orders", syncKeyColumnName = null) // Should auto-detect "id"
        )

        client = createSyncTestClientWithCustomPK(
            db = db,
            userSub = "test-user",
            deviceId = "test-device",
            syncTables = syncTables
        )

        // Bootstrap should succeed with auto-detected primary keys
        val bootstrapResult = client.bootstrap(userId = "test-user", sourceId = "test-device")
        assertTrue("Bootstrap should succeed with auto-detected primary keys", bootstrapResult.isSuccess)

        // Insert test data
        db.execSQL("""
            INSERT INTO custom_users (uuid, name, email) 
            VALUES ('auto-user-456', 'Jane Smith', 'jane@example.com')
        """.trimIndent())

        // Verify sync metadata uses the auto-detected primary key
        val pendingChanges = db.prepare("SELECT table_name, pk_uuid FROM _sync_pending WHERE table_name = 'custom_users'").use { st ->
            val changes = mutableListOf<Pair<String, String>>()
            while (st.step()) {
                changes.add(st.getText(0) to st.getText(1))
            }
            changes
        }

        assertEquals("Should have one pending change", 1, pendingChanges.size)
        assertEquals("custom_users", pendingChanges[0].first)
        assertEquals("Should use auto-detected uuid as primary key", "auto-user-456", pendingChanges[0].second)
    }

    private suspend fun verifyTriggersCreated() {
        // Check that triggers exist for all tables
        val triggerNames = db.prepare("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name").use { st ->
            val names = mutableListOf<String>()
            while (st.step()) {
                names.add(st.getText(0))
            }
            names
        }

        // Should have INSERT, UPDATE, DELETE triggers for each table
        val expectedTriggers = listOf(
            "trg_custom_users_ai", "trg_custom_users_au", "trg_custom_users_ad",
            "trg_products_ai", "trg_products_au", "trg_products_ad",
            "trg_orders_ai", "trg_orders_au", "trg_orders_ad"
        )

        expectedTriggers.forEach { expectedTrigger ->
            assertTrue(
                "Trigger $expectedTrigger should exist. Found triggers: $triggerNames",
                triggerNames.contains(expectedTrigger)
            )
        }
    }

    private suspend fun verifyPendingChanges() {
        val pendingChanges = db.prepare("SELECT table_name, pk_uuid, op FROM _sync_pending ORDER BY table_name").use { st ->
            val changes = mutableListOf<Triple<String, String, String>>()
            while (st.step()) {
                changes.add(Triple(st.getText(0), st.getText(1), st.getText(2)))
            }
            changes
        }

        assertEquals("Should have 3 pending changes", 3, pendingChanges.size)

        // Verify custom_users uses uuid as primary key
        val userChange = pendingChanges.find { it.first == "custom_users" }
        assertEquals("custom_users should use uuid as primary key", "user-123", userChange?.second)
        assertEquals("INSERT", userChange?.third)

        // Verify products uses code as primary key
        val productChange = pendingChanges.find { it.first == "products" }
        assertEquals("products should use code as primary key", "PROD-001", productChange?.second)
        assertEquals("INSERT", productChange?.third)

        // Verify orders uses id as primary key (should be auto-generated integer)
        val orderChange = pendingChanges.find { it.first == "orders" }
        assertTrue("orders should use integer id as primary key", orderChange?.second?.toIntOrNull() != null)
        assertEquals("INSERT", orderChange?.third)
    }

    private suspend fun verifyRowMetadata() {
        val rowMeta = db.prepare("SELECT table_name, pk_uuid FROM _sync_row_meta ORDER BY table_name").use { st ->
            val meta = mutableListOf<Pair<String, String>>()
            while (st.step()) {
                meta.add(st.getText(0) to st.getText(1))
            }
            meta
        }

        assertEquals("Should have 3 row metadata entries", 3, rowMeta.size)

        // Verify metadata uses correct primary key values
        val userMeta = rowMeta.find { it.first == "custom_users" }
        assertEquals("custom_users metadata should use uuid", "user-123", userMeta?.second)

        val productMeta = rowMeta.find { it.first == "products" }
        assertEquals("products metadata should use code", "PROD-001", productMeta?.second)

        val orderMeta = rowMeta.find { it.first == "orders" }
        assertTrue("orders metadata should use integer id", orderMeta?.second?.toIntOrNull() != null)
    }
}
