package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class TableInfoProviderTest {

    @Before
    fun setUp() {
        if (skipAllTest) {
            throw UnsupportedOperationException("(TEMPORARY setUp) Not implemented yet")
        }
    }

    @Test
    fun parses_regular_table_schema() = runBlockingTest {
        val db = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))

        db.execSQL(
            """
            CREATE TABLE ext_users (
              id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              email TEXT UNIQUE,
              avatar BLOB,
              created_at TEXT DEFAULT (datetime('now'))
            )
            """.trimIndent()
        )

        val ti = TableInfoProvider.get(db, "ext_users")

        // Basic
        assertEquals("ext_users", ti.table)
        assertEquals(listOf("id", "name", "email", "avatar", "created_at"), ti.columnNamesLower)

        // Types map echoes declared types
        assertEquals("text", ti.typesByNameLower["id"]?.lowercase())
        assertEquals("text", ti.typesByNameLower["name"]?.lowercase())
        assertEquals("text", ti.typesByNameLower["email"]?.lowercase())
        assertEquals("blob", ti.typesByNameLower["avatar"]?.lowercase())

        // PK
        assertNotNull(ti.primaryKey)
        assertEquals("id", ti.primaryKey?.name?.lowercase())
        assertTrue(!ti.primaryKeyIsBlob)
    }

    @Test
    fun detects_blob_primary_key() = runBlockingTest {
        val db = SafeSQLiteConnection(BundledSQLiteDriver().open(":memory:"))

        db.execSQL(
            """
            CREATE TABLE blobs (
              id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),
              note TEXT
            )
            """.trimIndent()
        )

        val ti = TableInfoProvider.get(db, "blobs")
        assertEquals("blobs", ti.table)
        assertEquals(listOf("id", "note"), ti.columnNamesLower)
        assertEquals("blob", ti.typesByNameLower["id"]?.lowercase())
        assertEquals("text", ti.typesByNameLower["note"]?.lowercase())
        assertEquals("id", ti.primaryKey?.name?.lowercase())
        assertTrue(ti.primaryKeyIsBlob)

        // Cache hit path sanity
        val ti2 = TableInfoProvider.get(db, "BLOBS") // different case
        assertEquals(ti.columnNamesLower, ti2.columnNamesLower)
        assertEquals(ti.typesByNameLower, ti2.typesByNameLower)
        assertEquals(ti.primaryKeyIsBlob, ti2.primaryKeyIsBlob)
    }
}

