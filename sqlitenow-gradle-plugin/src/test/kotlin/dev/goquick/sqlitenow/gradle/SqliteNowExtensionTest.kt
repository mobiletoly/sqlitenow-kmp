package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.DatabaseConfig
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.gradle.testfixtures.ProjectBuilder
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class SqliteNowExtensionTest {

    @Test
    @DisplayName("DatabaseConfig exposes stable defaults")
    fun databaseConfigExposesStableDefaults() {
        val project = ProjectBuilder.builder().build()
        val config = DatabaseConfig("AppDatabase", project.objects)

        assertEquals("AppDatabase", config.name)
        assertEquals("", config.packageName.get())
        assertFalse(config.debug.get())
        assertFalse(config.schemaDatabaseFile.isPresent)
    }

    @Test
    @DisplayName("SqliteNowExtension keeps multiple database declarations isolated")
    fun sqliteNowExtensionKeepsMultipleDatabaseDeclarationsIsolated() {
        val project = ProjectBuilder.builder().build()
        val extension = SqliteNowExtension(project.objects)

        val alpha = extension.databases.create("AlphaDatabase").apply {
            packageName.set("fixture.alpha.db")
            debug.set(true)
        }
        val beta = extension.databases.create("BetaDatabase").apply {
            packageName.set("fixture.beta.db")
        }

        assertEquals(listOf("AlphaDatabase", "BetaDatabase"), extension.databases.names.toList())
        assertEquals("fixture.alpha.db", alpha.packageName.get())
        assertTrue(alpha.debug.get())
        assertEquals("fixture.beta.db", beta.packageName.get())
        assertFalse(beta.debug.get())
        assertFalse(alpha.schemaDatabaseFile.isPresent)
        assertFalse(beta.schemaDatabaseFile.isPresent)
    }
}
