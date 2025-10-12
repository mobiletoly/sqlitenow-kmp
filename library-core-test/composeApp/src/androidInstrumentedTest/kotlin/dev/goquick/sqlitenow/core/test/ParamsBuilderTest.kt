package dev.goquick.sqlitenow.core.test

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Verifies the generated Params.Builder DSL for execute statements.
 */
@RunWith(AndroidJUnit4::class)
class ParamsBuilderTest {

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
    fun builderConstructsParams() = runBlocking {
        database.open()

        val inserted = database.person.add.one {
            firstName = "Builder"
            lastName = "Dsl"
            email = "builder@example.com"
            phone = "+15555555555"
            birthDate = LocalDate(1995, 5, 5)
        }

        assertEquals("Builder", inserted.firstName)
        assertEquals("Dsl", inserted.lastName)
        assertEquals("builder@example.com", inserted.email)
    }

    @Test
    fun builderRequiresRequiredFields() {
        val builder = PersonQuery.Add.Params.Builder().apply {
            firstName = "Missing"
        }

        assertThrows(IllegalStateException::class.java) {
            builder.build()
        }
    }
}
