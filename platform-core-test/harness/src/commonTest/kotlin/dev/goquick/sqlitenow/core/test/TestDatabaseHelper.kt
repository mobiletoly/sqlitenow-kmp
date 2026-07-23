package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.common.PlatformType
import dev.goquick.sqlitenow.common.platform
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import kotlin.random.Random

/**
 * Helper object to create LibraryTestDatabase instances with all required adapters.
 * This ensures consistent database setup across all test files.
 */
object TestDatabaseHelper {
    private var webDatabaseSequence: Long = 0

    /**
     * Creates a LibraryTestDatabase instance with all required adapters configured.
     * Uses the working configuration from BasicCollectionTest as the reference.
     */
    fun createDatabase(dbName: String = ":memory:", debug: Boolean = true) =
        createLibraryTestDatabase(dbName = dbName, debug = debug)

    fun createDatabaseLease(debug: Boolean = true): TestDatabaseLease {
        val dbName = if (platform() == PlatformType.JS) {
            webDatabaseSequence += 1
            "phase7-platform-core-${webDatabaseSequence}-${Random.nextInt()}.db"
        } else {
            ":memory:"
        }
        return TestDatabaseLease(
            database = createDatabase(dbName = dbName, debug = debug),
            dbName = dbName,
        )
    }
}

class TestDatabaseLease internal constructor(
    val database: LibraryTestDatabase,
    private val dbName: String,
) {
    suspend fun closeAndCleanup() {
        try {
            database.close()
        } finally {
            cleanupPlatformHarnessDatabase(dbName)
        }
    }
}

internal expect suspend fun cleanupPlatformHarnessDatabase(dbName: String)
