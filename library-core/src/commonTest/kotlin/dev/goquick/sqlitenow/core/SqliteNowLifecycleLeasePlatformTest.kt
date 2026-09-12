package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.common.initializePlatformTestContext
import dev.goquick.sqlitenow.common.platform
import dev.goquick.sqlitenow.common.PlatformType
import dev.goquick.sqlitenow.common.resolveDatabasePath
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlinx.coroutines.test.runTest

class SqliteNowLifecycleLeasePlatformTest {
    @Test
    fun lifecycleLeaseUsesPlatformOwnershipBoundaryAndReleases() = runTest {
        initializePlatformTestContext()
        val path = resolveDatabasePath(
            dbName = "sqlitenow-coordinatedopening-lifecycle-platform-test.sqlite3",
            appName = "SqliteNowLifecycleTest",
        )
        val first = acquireSqliteNowLifecycleLease(path)
        if (platform() == PlatformType.JS) {
            try {
                // Browser ownership is acquired inside the database worker. Node databases are
                // transient object instances, so neither runtime may share a pre-worker name lock.
                acquireSqliteNowLifecycleLease(path).close()
            } finally {
                first.close()
            }
            acquireSqliteNowLifecycleLease(path).close()
            return@runTest
        }

        try {
            assertFailsWith<SqliteNowLifecycleBusyException> {
                acquireSqliteNowLifecycleLease(path)
            }
        } finally {
            first.close()
        }

        acquireSqliteNowLifecycleLease(path).close()
    }
}
