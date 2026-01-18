package dev.goquick.sqlitenow.core.test

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAggregateSummary
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import dev.goquick.sqlitenow.core.test.db.PersonSummary
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class MapToAnnotationTest {

    private lateinit var database: LibraryTestDatabase

    @Before
    fun setup() {
        database = TestDatabaseHelper.createDatabase()
    }

    @After
    fun teardown() {
        runBlocking { database.close() }
    }

    @Test
    fun selectAllMapToSummaryReturnsMappedData() {
        runBlocking {
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

            assertTrue("Expected at least one summary", summaries.isNotEmpty())
            val summary = summaries.first { it.id == inserted.id }
            assertEquals(PersonSummary(inserted.id, "Map Target"), summary)
        }
    }

    @Test
    fun selectAllDescReturnsMappedData() {
        runBlocking {
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

            assertTrue("Expected at least one summary from view", summaries.isNotEmpty())
            val summary = summaries.first { it.id == inserted.id }
            assertEquals(PersonSummary(inserted.id, "Desc Target"), summary)
        }
    }

    @Test
    fun selectAggregatedStatsReturnsMappedData() {
        runBlocking {
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
}
