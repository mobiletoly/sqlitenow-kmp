package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.oversqlite.BundleChangeWatchMode
import dev.goquick.sqlitenow.oversqlite.platform.generated.RealServerGeneratedDatabase
import dev.goquick.sqlitenow.oversqlite.platform.generated.VersionBasedDatabaseMigrations
import kotlin.test.Test
import kotlin.test.assertEquals

class GeneratedAutomaticDownloadConfigTest {
    @Test
    fun generatedAutomaticDownloadConfigCanOptIntoWatch() {
        val database = RealServerGeneratedDatabase(
            dbName = ":memory:",
            migration = VersionBasedDatabaseMigrations(),
        )

        val automaticConfig = database.buildOversqliteAutomaticDownloadConfig(
            automaticDownloadIntervalMillis = 25,
            bundleChangeWatchMode = BundleChangeWatchMode.AUTO,
            bundleChangeWatchReconnectMinMillis = 10,
            bundleChangeWatchReconnectMaxMillis = 20,
        )

        assertEquals(25, automaticConfig.automaticDownloadIntervalMillis)
        assertEquals(BundleChangeWatchMode.AUTO, automaticConfig.bundleChangeWatchMode)
        assertEquals(10, automaticConfig.bundleChangeWatchReconnectMinMillis)
        assertEquals(20, automaticConfig.bundleChangeWatchReconnectMaxMillis)
    }
}
