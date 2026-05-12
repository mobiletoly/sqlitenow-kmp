package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.test.db.PersonQuery
import dev.goquick.sqlitenow.core.test.db.PersonRow
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.datetime.LocalDate
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class GeneratedDeleteReopenInvalidationTest {

    @Test
    fun deleteReEmitsReactiveQueryAfterSameDatabaseInstanceReopens() = runTest {
        val dbPath = createTempDirectory("generated-delete-reopen-invalidation").resolve("test.db").toString()
        val database = createLibraryTestDatabase(dbName = dbPath, debug = true)
        try {
            database.open()
            val person = database.person.add.one(
                PersonQuery.Add.Params(
                    email = "delete-reopen@example.com",
                    firstName = "Delete",
                    lastName = "Reopen",
                    phone = null,
                    birthDate = LocalDate(1990, 1, 1),
                ),
            )
            database.close()

            database.open()

            val emissions = mutableListOf<List<PersonRow>>()
            val collector = launch {
                database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                    .asFlow()
                    .collect { rows ->
                        emissions += rows
                    }
            }
            try {
                assertTrue(
                    awaitConditionWithRealTimeout(timeoutMs = 1_000) { emissions.size >= 1 },
                    "Flow should emit the initial result after reopen",
                )
                assertEquals(1, emissions.first().size, "Initial emission should include the seeded person")

                database.person.deleteByIds(PersonQuery.DeleteByIds.Params(ids = listOf(person.id)))

                assertTrue(
                    awaitConditionWithRealTimeout(timeoutMs = 1_000) { emissions.size >= 2 },
                    "Generated DELETE should re-emit after reopen",
                )
                assertEquals(emptyList(), emissions.last(), "DELETE emission should reflect the removed row")
            } finally {
                collector.cancelAndJoin()
            }
        } finally {
            if (database.isOpen()) {
                database.close()
            }
        }
    }
}
