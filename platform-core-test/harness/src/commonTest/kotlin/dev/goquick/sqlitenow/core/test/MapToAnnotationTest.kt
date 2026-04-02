package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAggregateSummary
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import dev.goquick.sqlitenow.core.test.db.PersonSummary
import kotlinx.datetime.LocalDate
import kotlin.test.*

class MapToAnnotationTest {

    private lateinit var database: LibraryTestDatabase

    private fun runDatabaseTest(block: suspend () -> Unit) = runPlatformTest {
        database = TestDatabaseHelper.createDatabase()
        try {
            block()
        } finally {
            database.close()
        }
    }

    @Test
    fun selectAllMapToSummaryReturnsMappedData() = runDatabaseTest {
            database.open()

            val params = PersonQuery.Add.Params(
                email = "map-to@example.com",
                firstName = "Map",
                lastName = "Target",
                phone = null,
                birthDate = LocalDate(1992, 4, 20),
            )
            val inserted = database.person.add.one(params)

            val summaries = database.person.selectAllAsc.asList()

            assertTrue(summaries.isNotEmpty(), "Expected at least one summary")
            val summary = summaries.first { it.id == inserted.id }
            assertEquals(PersonSummary(inserted.id, "Map Target"), summary)
    }

    @Test
    fun selectAllDescReturnsMappedData() = runDatabaseTest {
            database.open()

            val params = PersonQuery.Add.Params(
                email = "map-to-desc@example.com",
                firstName = "Desc",
                lastName = "Target",
                phone = null,
                birthDate = LocalDate(1993, 5, 15),
            )
            val inserted = database.person.add.one(params)

            val summaries = database.person.selectAllDesc.asList()

            assertTrue(summaries.isNotEmpty(), "Expected at least one summary from view")
            val summary = summaries.first { it.id == inserted.id }
            assertEquals(PersonSummary(inserted.id, "Desc Target"), summary)
    }

    @Test
    fun selectAggregatedStatsReturnsMappedData() = runDatabaseTest {
            database.open()

            database.person.add.one(
                PersonQuery.Add.Params(
                    email = "agg-1@example.com",
                    firstName = "Amy",
                    lastName = "Alpha",
                    phone = null,
                    birthDate = LocalDate(1990, 1, 1),
                )
            )
            database.person.add.one(
                PersonQuery.Add.Params(
                    email = "agg-2@example.com",
                    firstName = "Brian",
                    lastName = "Beta",
                    phone = null,
                    birthDate = LocalDate(1991, 2, 2),
                )
            )

            val stats: PersonAggregateSummary = database.person.selectAggregatedStats.asOne()
            assertEquals(2L, stats.totalCount)
            assertEquals((3 + 5) / 2.0, stats.averageFirstNameLength, 0.0001)
    }
}
