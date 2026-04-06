package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.sqlite.SqliteException
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame

class StatementCacheTest {
    @Test
    fun statementCache_rebindsWithoutLeakingPriorBindings() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        db.execSQL("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")

        val cache = StatementCache(db)
        try {
            val first = cache.get("INSERT INTO items(id, value) VALUES(?, ?)")
            first.bindLong(1, 1)
            first.bindText(2, "one")
            first.step()

            val second = cache.get("INSERT INTO items(id, value) VALUES(?, ?)")
            assertSame(first, second)
            second.bindLong(1, 2)
            second.bindText(2, "two")
            second.step()

            val values = mutableListOf<String>()
            db.prepare("SELECT value FROM items ORDER BY id").use { st ->
                while (st.step()) {
                    values += st.getText(0)
                }
            }
            assertEquals(listOf("one", "two"), values)
        } finally {
            cache.close()
            db.close()
        }
    }

    @Test
    fun statementCache_failedExecutionDoesNotPoisonLaterReuse() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        db.execSQL("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")

        val cache = StatementCache(db)
        try {
            val insert = cache.get("INSERT INTO items(id, value) VALUES(?, ?)")
            insert.bindLong(1, 1)
            insert.bindText(2, "one")
            insert.step()

            val duplicate = cache.get("INSERT INTO items(id, value) VALUES(?, ?)")
            duplicate.bindLong(1, 1)
            duplicate.bindText(2, "duplicate")
            assertFailsWith<SqliteException> {
                duplicate.step()
            }

            val recovery = cache.get("INSERT INTO items(id, value) VALUES(?, ?)")
            recovery.bindLong(1, 2)
            recovery.bindText(2, "two")
            recovery.step()

            assertEquals(2L, db.prepare("SELECT COUNT(*) FROM items").use { st ->
                check(st.step())
                st.getLong(0)
            })
        } finally {
            cache.close()
            db.close()
        }
    }

    @Test
    fun statementCache_closeOwnsStatementLifetime() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        val cache = StatementCache(db)
        val statement = cache.get("SELECT 1")
        assertSame(statement, cache.get("SELECT 1"))

        cache.close()

        assertFailsWith<IllegalStateException> {
            cache.get("SELECT 1")
        }
        assertFailsWith<Throwable> {
            statement.reset()
        }

        db.close()
    }
}
