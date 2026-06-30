package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.DatabaseConfig
import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS
import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_PACKAGE_MINIMUM_IOS
import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_PACKAGE_MINIMUM_MACOS
import java.io.File
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
        assertFalse(config.oversqlite.get())
        assertFalse(config.schemaDatabaseFile.isPresent)
        assertFalse(config.swiftPackage.enabled.get())
        assertEquals("AppDatabaseSQLiteNow", config.swiftPackage.packageName.get())
        assertEquals("AppDatabaseSQLiteNow", config.swiftPackage.swiftTargetName.get())
        assertEquals(DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS, config.swiftPackage.requestedAppleTargets.get())
        assertEquals(DEFAULT_SWIFT_PACKAGE_MINIMUM_IOS, config.swiftPackage.minimumIos.get())
        assertEquals(DEFAULT_SWIFT_PACKAGE_MINIMUM_MACOS, config.swiftPackage.minimumMacos.get())
        assertEquals("dynamic", config.swiftPackage.frameworkMode.get())
        assertFalse(config.swiftPackage.runtime.isPresent)
        assertFalse(config.swiftPackage.runtimeArtifact.kind.isPresent)
        assertFalse(config.swiftPackage.runtimeArtifact.localXcframeworkDirectory.isPresent)
        assertFalse(config.swiftPackage.runtimeArtifact.localZipFile.isPresent)
        assertFalse(config.swiftPackage.runtimeArtifact.remoteZipUrl.isPresent)
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

    @Test
    @DisplayName("swiftPackage action enables and isolates package configuration")
    fun swiftPackageActionEnablesAndIsolatesPackageConfiguration() {
        val project = ProjectBuilder.builder().build()
        val extension = SqliteNowExtension(project.objects)

        val alpha = extension.databases.create("AlphaDatabase").apply {
            swiftPackage { config ->
                config.packageName.set("AlphaDatabasePackage")
                config.swiftTargetName.set("AlphaDatabaseTarget")
                config.minimumIos.set("16")
            }
        }
        val beta = extension.databases.create("BetaDatabase")

        assertTrue(alpha.swiftPackage.enabled.get())
        assertEquals("AlphaDatabasePackage", alpha.swiftPackage.packageName.get())
        assertEquals("AlphaDatabaseTarget", alpha.swiftPackage.swiftTargetName.get())
        assertEquals("16", alpha.swiftPackage.minimumIos.get())

        assertFalse(beta.swiftPackage.enabled.get())
        assertEquals("BetaDatabaseSQLiteNow", beta.swiftPackage.packageName.get())
        assertEquals("BetaDatabaseSQLiteNow", beta.swiftPackage.swiftTargetName.get())
        assertEquals(DEFAULT_SWIFT_PACKAGE_MINIMUM_IOS, beta.swiftPackage.minimumIos.get())
    }

    @Test
    @DisplayName("runtimeArtifact helpers set the selected artifact kind")
    fun runtimeArtifactHelpersSetSelectedArtifactKind() {
        val project = ProjectBuilder.builder().build()
        val config = DatabaseConfig("AppDatabase", project.objects)
        val localXcframework = File("runtime/SQLiteNowCoreRuntime.xcframework")
        val localZip = File("Artifacts/SQLiteNowCoreRuntime.zip")

        config.swiftPackage.runtimeArtifact.localXcframework(localXcframework)
        assertEquals("localXcframework", config.swiftPackage.runtimeArtifact.kind.get())
        assertTrue(
            config.swiftPackage.runtimeArtifact.localXcframeworkDirectory.get().asFile.toInvariantPath()
                .endsWith(localXcframework.toInvariantPath())
        )

        config.swiftPackage.runtimeArtifact.localZip(localZip)
        assertEquals("localZip", config.swiftPackage.runtimeArtifact.kind.get())
        assertTrue(
            config.swiftPackage.runtimeArtifact.localZipFile.get().asFile.toInvariantPath()
                .endsWith(localZip.toInvariantPath())
        )

        config.swiftPackage.runtimeArtifact.remoteZip("https://example.com/SQLiteNowCoreRuntime.zip")
        assertEquals("remoteZip", config.swiftPackage.runtimeArtifact.kind.get())
        assertEquals(
            "https://example.com/SQLiteNowCoreRuntime.zip",
            config.swiftPackage.runtimeArtifact.remoteZipUrl.get(),
        )
    }

    private fun File.toInvariantPath(): String =
        path.replace(File.separatorChar, '/')
}
