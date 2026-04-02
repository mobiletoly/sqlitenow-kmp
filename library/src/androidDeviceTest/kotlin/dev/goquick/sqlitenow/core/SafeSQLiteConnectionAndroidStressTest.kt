package dev.goquick.sqlitenow.core

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.Assume.assumeTrue
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File
import java.util.UUID
import kotlin.test.assertEquals

@RunWith(AndroidJUnit4::class)
class SafeSQLiteConnectionAndroidStressTest {
    @Test
    fun stressAliasStarReadsDuringSuspendedTransactions() = runBlocking {
        val config = requireStressConfig()
        val dbFile = createTempDbFile()
        val db = BundledSqliteConnectionProvider.openConnection(dbFile.absolutePath, debug = true)
        try {
            createDailyLogTable(db)
            seedDailyLogs(db, config.rowCount, config.dateValue)

            withTimeout(config.timeoutMillis) {
                coroutineScope {
                    val jobs = buildList {
                        repeat(config.writerCoroutines) { writerIndex ->
                            add(
                                launch(Dispatchers.Default) {
                                    repeat(config.writerIterations) { iteration ->
                                        val rowId = ((writerIndex + iteration) % config.rowCount) + 1L
                                        db.transaction(TransactionMode.IMMEDIATE) {
                                            incrementDailyLogCounter(db, rowId)
                                            delay(config.transactionPauseMillis)
                                            if (iteration % config.transactionReadEvery == 0) {
                                                assertEquals(
                                                    config.rowCount.toLong(),
                                                    queryDailyLogsByDateWithAliasStar(db, config.dateValue),
                                                )
                                            }
                                        }
                                    }
                                }
                            )
                        }

                        repeat(config.readerCoroutines) { readerIndex ->
                            add(
                                launch(Dispatchers.Default) {
                                    repeat(config.readerIterations) { iteration ->
                                        assertEquals(
                                            config.rowCount.toLong(),
                                            queryDailyLogsByDateWithAliasStar(db, config.dateValue),
                                        )
                                        if (iteration % config.pointReadEvery == 0) {
                                            readDailyLogById(
                                                db = db,
                                                rowId = ((readerIndex + iteration) % config.rowCount) + 1L,
                                            )
                                        }
                                    }
                                }
                            )
                        }
                    }
                    jobs.joinAll()
                }
            }

            assertEquals(config.rowCount.toLong(), queryDailyLogsByDateWithAliasStar(db, config.dateValue))
            assertEquals(
                (config.writerCoroutines * config.writerIterations).toLong(),
                queryTotalCounter(db, config.dateValue),
            )
        } finally {
            db.close()
            deleteDbArtifacts(dbFile)
        }
    }

    private fun requireStressConfig(): StressConfig {
        val args = InstrumentationRegistry.getArguments()
        val enabled = args.getString("sqlitenowStress")?.trim().orEmpty()
        assumeTrue(
            "opt-in stress test disabled; pass -Pandroid.testInstrumentationRunnerArguments.sqlitenowStress=true",
            enabled.equals("true", ignoreCase = true) || enabled == "1",
        )
        return StressConfig(
            rowCount = args.getString("sqlitenowStressRowCount")?.toIntOrNull() ?: 32,
            writerCoroutines = args.getString("sqlitenowStressWriterCoroutines")?.toIntOrNull() ?: 2,
            readerCoroutines = args.getString("sqlitenowStressReaderCoroutines")?.toIntOrNull() ?: 6,
            writerIterations = args.getString("sqlitenowStressWriterIterations")?.toIntOrNull() ?: 4_000,
            readerIterations = args.getString("sqlitenowStressReaderIterations")?.toIntOrNull() ?: 8_000,
            transactionPauseMillis = args.getString("sqlitenowStressPauseMillis")?.toLongOrNull() ?: 1L,
            transactionReadEvery = args.getString("sqlitenowStressTransactionReadEvery")?.toIntOrNull() ?: 4,
            pointReadEvery = args.getString("sqlitenowStressPointReadEvery")?.toIntOrNull() ?: 8,
            timeoutMillis = args.getString("sqlitenowStressTimeoutMillis")?.toLongOrNull() ?: 60_000L,
            dateValue = args.getString("sqlitenowStressDateValue")?.toLongOrNull() ?: 20_269L,
        )
    }

    private fun createTempDbFile(): File {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        return File(context.cacheDir, "sqlitenow-stress-${UUID.randomUUID()}.db")
    }

    private suspend fun createDailyLogTable(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE daily_log (
                id INTEGER PRIMARY KEY NOT NULL,
                date INTEGER NOT NULL,
                counter INTEGER NOT NULL,
                notes TEXT,
                activity_id TEXT NOT NULL,
                group_id TEXT
            )
            """.trimIndent(),
        )
    }

    private suspend fun seedDailyLogs(
        db: SafeSQLiteConnection,
        rowCount: Int,
        dateValue: Long,
    ) = db.withExclusiveAccess {
        db.prepare(
            """
            INSERT INTO daily_log (
                id,
                date,
                counter,
                notes,
                activity_id,
                group_id
            ) VALUES (?, ?, ?, ?, ?, ?)
            """.trimIndent(),
        ).use { statement ->
            repeat(rowCount) { index ->
                statement.bindLong(1, index + 1L)
                statement.bindLong(2, dateValue)
                statement.bindLong(3, 0L)
                statement.bindText(4, "note-$index")
                statement.bindText(5, "activity-${index % 4}")
                statement.bindText(6, "group-${index % 3}")
                statement.step()
                statement.reset()
                statement.clearBindings()
            }
        }
    }

    private suspend fun incrementDailyLogCounter(
        db: SafeSQLiteConnection,
        rowId: Long,
    ) {
        db.prepare("UPDATE daily_log SET counter = counter + 1 WHERE id = ?").use { statement ->
            statement.bindLong(1, rowId)
            statement.step()
        }
    }

    private suspend fun queryDailyLogsByDateWithAliasStar(
        db: SafeSQLiteConnection,
        dateValue: Long,
    ): Long = db.withExclusiveAccess {
        db.prepare("SELECT dl.* FROM daily_log dl WHERE dl.date = ?").use { statement ->
            statement.bindLong(1, dateValue)
            var rowCount = 0L
            while (statement.step()) {
                statement.getLong(0)
                statement.getLong(1)
                statement.getLong(2)
                if (!statement.isNull(3)) {
                    statement.getText(3)
                }
                statement.getText(4)
                if (!statement.isNull(5)) {
                    statement.getText(5)
                }
                rowCount++
            }
            rowCount
        }
    }

    private suspend fun readDailyLogById(
        db: SafeSQLiteConnection,
        rowId: Long,
    ) = db.withExclusiveAccess {
        db.prepare("SELECT * FROM daily_log WHERE id = ?").use { statement ->
            statement.bindLong(1, rowId)
            if (statement.step()) {
                statement.getLong(0)
                statement.getLong(1)
                statement.getLong(2)
                if (!statement.isNull(3)) {
                    statement.getText(3)
                }
                statement.getText(4)
                if (!statement.isNull(5)) {
                    statement.getText(5)
                }
            }
        }
    }

    private suspend fun queryTotalCounter(
        db: SafeSQLiteConnection,
        dateValue: Long,
    ): Long = db.withExclusiveAccess {
        db.prepare("SELECT COALESCE(SUM(counter), 0) FROM daily_log WHERE date = ?").use { statement ->
            statement.bindLong(1, dateValue)
            check(statement.step())
            statement.getLong(0)
        }
    }

    private fun deleteDbArtifacts(dbFile: File) {
        dbFile.delete()
        File("${dbFile.absolutePath}-journal").delete()
        File("${dbFile.absolutePath}-wal").delete()
        File("${dbFile.absolutePath}-shm").delete()
    }

    private data class StressConfig(
        val rowCount: Int,
        val writerCoroutines: Int,
        val readerCoroutines: Int,
        val writerIterations: Int,
        val readerIterations: Int,
        val transactionPauseMillis: Long,
        val transactionReadEvery: Int,
        val pointReadEvery: Int,
        val timeoutMillis: Long,
        val dateValue: Long,
    )
}
