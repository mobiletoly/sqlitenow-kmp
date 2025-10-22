package dev.goquick.sqlitenow.core.test

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.ParentChildDoc
import dev.goquick.sqlitenow.core.test.db.ParentMainDoc
import dev.goquick.sqlitenow.core.test.db.ParentQuery
import dev.goquick.sqlitenow.core.test.db.ParentWithChildrenDoc
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Integration tests that cover the parent_with_children_view code generation. These tests ensure
 * our dynamic field filtering keeps only the expected top-level fields when nested views contribute
 * additional per-row mappings (e.g. child schedules).
 */
@RunWith(AndroidJUnit4::class)
class ParentViewCollectionTest {

    private lateinit var database: LibraryTestDatabase

    @Before
    fun setUp() {
        runBlocking {
            database = TestDatabaseHelper.createDatabase()
            database.open()
        }
    }

    @After
    fun tearDown() {
        runBlocking {
            database.close()
        }
    }

    @Test
    fun selectWithChildrenDoesNotExposeChildSchedule() = runBlocking {
        // Arrange database state
        database.transaction {
            val conn = database.connection()
            conn.execSQL("INSERT INTO parent_category(id, doc_id, title) VALUES (1, 'cat-doc-1', 'Wellness')")
            conn.execSQL("INSERT INTO parent_entity(id, doc_id, category_id) VALUES (10, 'pkg-doc-1', 1)")
            conn.execSQL("INSERT INTO child_entity(id, parent_doc_id, title) VALUES (100, 'pkg-doc-1', 'Stretching')")
            conn.execSQL("INSERT INTO child_schedule(id, child_id, frequency, start_day) VALUES (200, 100, 'weekly', 1)")
        }

        // Act: run generated query
        val results = database.parent.selectWithChildrenByDocId(
            ParentQuery.SelectWithChildrenByDocId.Params(docId = "pkg-doc-1")
        ).asList()

        // Assert top-level structure
        assertEquals(1, results.size)
        val row = results.single()
        assertEquals(ParentMainDoc(id = 10, docId = "pkg-doc-1", categoryId = 1), row.main)
        assertEquals(ParentChildDoc::class.java, row.children.first()::class.java)
        assertEquals("Wellness", row.category.title)

        // Ensure the collection mapping remains intact
        assertEquals(listOf(ParentChildDoc(id = 100, parentDocId = "pkg-doc-1", title = "Stretching")), row.children)

        // Verify no stray schedule property is exposed on the result data class
        val fieldNames = ParentWithChildrenDoc::class.java.declaredFields.map { it.name }
        assertTrue(
            "ParentWithChildrenDoc should not expose child schedule as a top-level field",
            "schedule" !in fieldNames
        )
    }
}
