package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlin.test.assertEquals

/** Shared schema and complete readback for the 14-table capacity authority. */
internal object SnapshotCapacityFixture {
    const val rows = 50_000
    val names = (0 until 14).map { "records_${it.toString().padStart(2, '0')}" }
    val syncTables = names.map { SyncTable(it, syncKeyColumnName = "id") }

    suspend fun createTables(db: SafeSQLiteConnection) {
        for ((index, name) in names.withIndex()) {
            val foreignKeys = if (index != 13) "" else (0..2).joinToString("") {
                ", parent_$it TEXT NOT NULL REFERENCES ${names[it]}(id) DEFERRABLE INITIALLY DEFERRED"
            }
            db.execSQL("CREATE TABLE $name(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL$foreignKeys)")
        }
    }

    suspend fun totalBusinessRows(db: SafeSQLiteConnection): Long = names.sumOf { name ->
        db.withPreparedStatement("SELECT count(*) FROM $name") { statement ->
            check(statement.step())
            statement.getLong(0)
        }
    }

    suspend fun assertCompleteRows(db: SafeSQLiteConnection, firstBundle: Long,
        replacement: Boolean, localIntent: Boolean, rowCount: Int = rows) {
        val prefix = if (replacement) "replacement" else "synthetic"
        for ((tableIndex, name) in names.withIndex()) {
            val expectedRows = if (tableIndex == 13) rowCount - 13 else 1
            db.withPreparedStatement("SELECT id,name${if (tableIndex == 13) ",parent_0,parent_1,parent_2" else ""} FROM $name ORDER BY id") { statement ->
                var index = 0
                while (statement.step()) {
                    val ordinal = if (tableIndex == 13) index + 13 else tableIndex
                    val id = if (tableIndex == 13) "row-${ordinal.toString().padStart(5, '0')}" else "parent"
                    assertEquals(id, statement.getText(0), "$name business key $index")
                    assertEquals(if (localIntent && ordinal == 49) "local-intent" else "$prefix-$ordinal",
                        statement.getText(1), "$name business payload $index")
                    if (tableIndex == 13) for (parent in 0..2) {
                        assertEquals("parent", statement.getText(parent + 2), "$name parent $parent at $index")
                    }
                    index++
                }
                assertEquals(expectedRows, index, "$name complete business readback")
            }
        }
        db.withPreparedStatement("SELECT table_name,json_extract(key_json,'$.id'),row_version,server_kind," +
            "json_extract(server_payload,'$.id.value'),json_extract(server_payload,'$.name.value') " +
            "FROM _sync_row_state ORDER BY table_name,key_json") { statement ->
            var index = 0
            while (statement.step()) {
                val tableIndex = if (index < 13) index else 13
                val id = if (index < 13) "parent" else "row-${index.toString().padStart(5, '0')}"
                assertEquals(names[tableIndex], statement.getText(0), "A table $index")
                assertEquals(id, statement.getText(1), "A key $index")
                val expectedBundle = if (index < 13) firstBundle else
                    firstBundle + 1 + (index - 13) / 200
                assertEquals(expectedBundle, statement.getLong(2), "A version $index")
                assertEquals("live", statement.getText(3), "A kind $index")
                assertEquals(id, statement.getText(4), "A payload key $index")
                assertEquals("$prefix-$index", statement.getText(5), "A payload name $index")
                index++
            }
            assertEquals(rowCount, index, "complete authoritative readback")
        }
    }
}
