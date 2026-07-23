package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.SqliteException
import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.CategoryQuery
import dev.goquick.sqlitenow.core.test.db.PersonAggregateResult
import dev.goquick.sqlitenow.core.test.db.PersonAggregateSummary
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import dev.goquick.sqlitenow.core.test.db.PersonSelectOneResult
import dev.goquick.sqlitenow.core.test.db.PersonSummary
import dev.goquick.sqlitenow.core.test.db.PersonSummaryResult
import dev.goquick.sqlitenow.core.test.db.SinglePersonSummary
import dev.goquick.sqlitenow.core.test.db.VersionBasedDatabaseMigrations
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.execute
import dev.goquick.sqlitenow.core.test.db.executeAsList
import dev.goquick.sqlitenow.core.test.db.executeReturningOne
import dev.goquick.sqlitenow.core.test.phase6.db.ParityValueQuery
import dev.goquick.sqlitenow.core.test.phase6.db.executeAsList
import dev.goquick.sqlitenow.core.test.phase6.db.executeReturningOne
import dev.goquick.sqlitenow.core.util.fromSqliteDate
import dev.goquick.sqlitenow.core.util.fromRfc3339String
import dev.goquick.sqlitenow.core.util.toSqliteDate
import dev.goquick.sqlitenow.core.util.toRfc3339String
import dev.goquick.sqlitenow.core.worker.SqliteWorkerSQLiteConnection
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.random.Random
import kotlin.time.Instant
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.withContext
import kotlinx.datetime.LocalDate
import kotlinx.serialization.json.Json

class GeneratedWorkerParityTest {
    @Test
    fun generatedCoreFixtureUsesOrdinaryDefaultWorker() = runTest {
        val dbName = phase7DefaultDbName("generated-core")
        cleanupGeneratedWorkerMigrationState(dbName)
        try {
            runScenario(dbName)
        } finally {
            cleanupGeneratedWorkerMigrationState(dbName)
        }
    }

    @Test
    fun generatedDebugWritePreservesWorkerSqliteExceptionAndItsDirectCause() = runTest {
        val workerDbName = phase7DefaultDbName("debug-error")
        cleanupGeneratedWorkerMigrationState(workerDbName)
        val database = createParityDatabase(workerDbName)
        database.open()
        try {
            val connection = database.connection()
            val params = CategoryQuery.Add.Params(
                name = "duplicate-generated-worker",
                description = null,
            )
            CategoryQuery.Add.executeReturningOne(
                conn = connection,
                params = params,
                sqlValueToCreatedAt = { Instant.fromRfc3339String(it) },
            )

            val failure = assertFailsWith<SqliteException> {
                CategoryQuery.Add.executeReturningOne(
                    conn = connection,
                    params = params,
                    sqlValueToCreatedAt = { Instant.fromRfc3339String(it) },
                )
            }
            assertTrue(failure.cause != null)
            assertFalse(failure.cause is SqliteException)
            assertTrue(failure.cause.toString().contains("SQLite worker"))
        } finally {
            database.close()
            cleanupGeneratedWorkerMigrationState(workerDbName)
        }
    }

    @Test
    fun generatedAsOneClosesBeforeDrainingMoreThanOneWorkerPage() = runTest {
        val workerDbName = phase7DefaultDbName("as-one-close")
        cleanupGeneratedWorkerMigrationState(workerDbName)
        val database = createParityDatabase(workerDbName)
        database.open()
        try {
            val connection = database.connection()
            connection.execSQL(
                """
                WITH RECURSIVE category_rows(value) AS (
                    SELECT 1
                    UNION ALL
                    SELECT value + 1 FROM category_rows WHERE value < 257
                )
                INSERT INTO category(id, name)
                SELECT value, printf('%06d', value) FROM category_rows
                """.trimIndent(),
            )

            assertEquals("000001", database.category.selectAll.asOne().name)
            val count = connection.prepare("SELECT count(*) FROM category")
            try {
                assertTrue(count.step())
                assertEquals(257L, count.getLong(0))
            } finally {
                count.close()
            }

            // SqliteWorkerDriverTest.hundredThousandRowsEarlyCloseAndFullDrainStayBounded
            // supplies the independent worker-owned observation for this same close path:
            // exactly one 64-row page is stepped, encoded, and transferred.
        } finally {
            database.close()
            cleanupGeneratedWorkerMigrationState(workerDbName)
        }
    }

    @Test
    fun closeFinalizesLiveCurrentStatementsForBothGeneratedProviders() = runTest {
        val workerDbName = phase7DefaultDbName("live-statement-close")
        cleanupGeneratedWorkerMigrationState(workerDbName)
        try {
            val database = createParityDatabase(workerDbName)
            database.open()
            val statement = database.connection().prepare(
                "SELECT name FROM category ORDER BY name",
            )
            var databaseClosed = false
            try {
                database.category.add.one(
                    CategoryQuery.Add.Params(
                        name = "close-current",
                        description = null,
                    ),
                )
                assertTrue(statement.step())
                assertEquals("close-current", statement.getText(0))
                database.close()
                databaseClosed = true
            } finally {
                if (!databaseClosed) {
                    statement.close()
                }
                database.close()
            }
        } finally {
            cleanupGeneratedWorkerMigrationState(workerDbName)
        }
    }

    private suspend fun runScenario(
        dbName: String,
    ): ParityOutcome {
        val database = createParityDatabase(dbName)
        database.open()
        try {
            val connection = database.connection()
            val worker = connection.ref as? SqliteWorkerSQLiteConnection
                ?: error("The ordinary generated web database did not use the worker runtime.")
            val metrics = worker.metricsForTest()
            assertTrue(metrics.runtimeKind in setOf("js-node-worker", "browser-worker"))
            assertEquals(
                if (metrics.runtimeKind == "js-node-worker") "memory" else "direct-opfs",
                metrics.storageMode,
            )
            val schema = schema(connection)
            assertNull(database.category.selectAll.asOneOrNull())
            assertFails {
                database.category.selectAll.asOne()
            }
            val first = CategoryQuery.Add.executeReturningOne(
                conn = connection,
                params = CategoryQuery.Add.Params(
                    name = "alpha",
                    description = "first",
                ),
                sqlValueToCreatedAt = { Instant.fromRfc3339String(it) },
            )
            assertEquals("alpha", first.name)
            assertEquals("alpha", database.category.selectAll.asOne().name)

            val person = database.person.add.one(
                PersonQuery.Add.Params(
                    email = "parity@example.com",
                    firstName = "before",
                    lastName = "person",
                    phone = null,
                    birthDate = LocalDate(2001, 2, 3),
                ),
            )
            assertEquals(LocalDate(2001, 2, 3), person.birthDate)
            val collectionPerson = database.person.add.one(
                PersonQuery.Add.Params(
                    email = "collection@example.com",
                    firstName = "second",
                    lastName = "collection",
                    phone = "123",
                    birthDate = null,
                ),
            )
            val collectionRows = database.person.selectAllByLastNames(
                PersonQuery.SelectAllByLastNames.Params(
                    lastNames = listOf("person", "collection"),
                ),
            ).asList()
            assertEquals(
                setOf(person.id, collectionPerson.id),
                collectionRows.map { it.id }.toSet(),
            )
            assertEquals(
                listOf("second collection", "before person"),
                database.person.selectAllAsc.asList().map { it.fullName },
            )

            PersonQuery.UpdateById.execute(
                conn = connection,
                params = PersonQuery.UpdateById.Params(
                    firstName = "after",
                    lastName = person.myLastName,
                    email = person.email,
                    phone = person.phone,
                    birthDate = person.birthDate,
                    id = person.id,
                ),
                birthDateToSqlValue = { it?.toString() },
            )
            val deleted = PersonQuery.DeleteByIdReturning.executeReturningOne(
                conn = connection,
                params = PersonQuery.DeleteByIdReturning.Params(person.id),
                sqlValueToBirthDate = { null },
                sqlValueToCreatedAt = { Instant.fromRfc3339String(it) },
            )
            assertEquals("after", deleted.myFirstName)

            database.transaction {
                CategoryQuery.Add.executeReturningOne(
                    conn = connection,
                    params = CategoryQuery.Add.Params(
                        name = "committed",
                        description = "transaction",
                    ),
                    sqlValueToCreatedAt = { Instant.fromRfc3339String(it) },
                )
            }
            assertFalse(connection.inTransaction())

            try {
                database.transaction {
                    CategoryQuery.Add.executeReturningOne(
                        conn = connection,
                        params = CategoryQuery.Add.Params(
                            name = "rolled-back",
                            description = null,
                        ),
                        sqlValueToCreatedAt = { Instant.fromRfc3339String(it) },
                    )
                    throw ParityRollback()
                }
            } catch (_: ParityRollback) {
                // Expected: this proves both providers roll generated writes back.
            }
            assertFalse(connection.inTransaction())

            val afterRollback = CategoryQuery.SelectAll.executeAsList(
                conn = connection,
                sqlValueToCreatedAt = { Instant.fromRfc3339String(it) },
            )
            assertEquals(listOf("alpha", "committed"), afterRollback.map { it.name })

            val flowValues = coroutineScope {
                database.enableTableChangeNotifications()
                val emissions = Channel<List<String>>(Channel.UNLIMITED)
                val collector = launch(Dispatchers.Default) {
                    database.category.selectAll.asFlow().collect { rows ->
                        emissions.send(rows.map { it.name }.sorted())
                    }
                }
                val initial = withRealTimeout(5_000) { emissions.receive() }
                assertEquals(listOf("alpha", "committed"), initial)
                database.category.add.one(
                    CategoryQuery.Add.Params(
                        name = "beta",
                        description = "second",
                    ),
                )
                val updated = withRealTimeout(5_000) { emissions.receive() }
                assertEquals(listOf("alpha", "beta", "committed"), updated)
                val duplicate = withContext(Dispatchers.Default) {
                    withTimeoutOrNull(250) { emissions.receive() }
                }
                assertEquals(null, duplicate, "Generated writes must notify exactly once.")
                collector.cancelAndJoin()
                listOf(initial, updated)
            }

            connection.execSQL(
                """
                CREATE TABLE parity_value(
                    id INTEGER PRIMARY KEY NOT NULL,
                    text_value TEXT NOT NULL,
                    nullable_value TEXT,
                    real_value REAL NOT NULL,
                    blob_value BLOB NOT NULL,
                    exact_value INTEGER NOT NULL
                )
                """.trimIndent(),
            )
            val minimum = ParityValueQuery.Add.executeReturningOne(
                conn = connection,
                params = ParityValueQuery.Add.Params(
                    textValue = "minimum",
                    nullableValue = null,
                    realValue = -1.25,
                    blobValue = byteArrayOf(0, 1, 127, -1),
                    exactValue = Long.MIN_VALUE,
                ),
            )
            assertEquals(Long.MIN_VALUE, minimum.exactValue)
            val maximum = ParityValueQuery.Add.executeReturningOne(
                conn = connection,
                params = ParityValueQuery.Add.Params(
                    textValue = "maximum",
                    nullableValue = "present",
                    realValue = 2.5,
                    blobValue = byteArrayOf(42),
                    exactValue = Long.MAX_VALUE,
                ),
            )
            assertEquals(Long.MAX_VALUE, maximum.exactValue)
            val typedValues = ParityValueQuery.SelectAll.executeAsList(connection)

            return ParityOutcome(
                schema = schema,
                returningName = first.name,
                updatedAndDeletedPersonName = deleted.myFirstName,
                namesAfterRollback = afterRollback.map { it.name },
                flowNames = flowValues,
                collectionPersonIds = collectionRows.map { it.id },
                adapterSummaryNames = database.person.selectAllAsc.asList().map { it.fullName },
                typedValues = typedValues.map {
                    TypedValueOutcome(
                        text = it.textValue,
                        nullableText = it.nullableValue,
                        real = it.realValue,
                        blob = it.blobValue.toList(),
                        exact = it.exactValue,
                    )
                },
            )
        } finally {
            database.close()
        }
    }

    private suspend fun schema(connection: SafeSQLiteConnection): List<String> {
        val statement = connection.prepare(
            """
            SELECT type || ':' || name
            FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
            ORDER BY type, name
            """.trimIndent(),
        )
        val schema = mutableListOf<String>()
        try {
            while (statement.step()) schema += statement.getText(0)
        } finally {
            statement.close()
        }
        return schema
    }
}

private fun createParityDatabase(
    dbName: String,
): LibraryTestDatabase =
    LibraryTestDatabase(
        dbName = dbName,
        migration = VersionBasedDatabaseMigrations(),
        debug = false,
        categoryAdapters = LibraryTestDatabase.CategoryAdapters(
            sqlValueToCreatedAt = { Instant.fromRfc3339String(it) },
        ),
        personAdapters = LibraryTestDatabase.PersonAdapters(
            birthDateToSqlValue = { it?.toSqliteDate() },
            sqlValueToBirthDate = { value ->
                value?.let { LocalDate.fromSqliteDate(it) }
            },
            personSummaryResultMapper = { raw: PersonSummaryResult ->
                PersonSummary(
                    id = raw.id,
                    fullName = "${raw.myFirstName} ${raw.myLastName}".trim(),
                )
            },
            personSelectOneResultMapper = { raw: PersonSelectOneResult ->
                SinglePersonSummary(
                    id = raw.id,
                    fullName = "${raw.myFirstName} ${raw.myLastName}".trim(),
                    age = 33,
                )
            },
            personAggregateResultMapper = { raw: PersonAggregateResult ->
                PersonAggregateSummary(
                    totalCount = raw.totalCount,
                    averageFirstNameLength = raw.avgFirstNameLength ?: 0.0,
                )
            },
        ),
        commentAdapters = LibraryTestDatabase.CommentAdapters(
            createdAtToSqlValue = { it.toRfc3339String() },
            tagsToSqlValue = { value -> value?.let { Json.encodeToString(it) } },
            sqlValueToTags = { value -> value?.let { Json.decodeFromString<List<String>>(it) } },
        ),
        personCategoryAdapters = LibraryTestDatabase.PersonCategoryAdapters(
            sqlValueToAssignedAt = { Instant.fromRfc3339String(it) },
        ),
        personAddressAdapters = LibraryTestDatabase.PersonAddressAdapters(
            addressTypeToSqlValue = { it.value },
            sqlValueToAddressType = { AddressType.from(it) },
            sqlValueToConstantTimestamp = { epochSeconds ->
                epochSeconds?.let { Instant.fromEpochSeconds(it) }
            },
        ),
    )

private fun phase7DefaultDbName(scenario: String): String =
    "__sqlitenow_phase7_${scenario}_${Random.nextInt().toUInt()}"

private data class ParityOutcome(
    val schema: List<String>,
    val returningName: String,
    val updatedAndDeletedPersonName: String,
    val namesAfterRollback: List<String>,
    val flowNames: List<List<String>>,
    val collectionPersonIds: List<Long>,
    val adapterSummaryNames: List<String>,
    val typedValues: List<TypedValueOutcome>,
)

private data class TypedValueOutcome(
    val text: String,
    val nullableText: String?,
    val real: Double,
    val blob: List<Byte>,
    val exact: Long,
)

private class ParityRollback : RuntimeException()
