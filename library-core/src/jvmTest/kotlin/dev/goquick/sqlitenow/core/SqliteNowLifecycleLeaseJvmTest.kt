package dev.goquick.sqlitenow.core

import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking
import kotlin.test.assertEquals
import kotlin.io.path.deleteIfExists
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest

class SqliteNowLifecycleLeaseJvmTest {
    @Test
    fun unlockFailureDoesNotRetainRegistryOwnershipAfterFileClose() = runTest {
        val database = Files.createTempFile("sqlitenow-lifecycle-unlock", ".sqlite3")
        val unlockFailure = IllegalStateException("LIFECYCLE_UNLOCK_SENTINEL")
        val lease = acquireSqliteNowLifecycleLease(database.toString())
        try {
            jvmAndroidLifecycleLockReleaseForTest = { throw unlockFailure }

            assertSame(unlockFailure, assertFailsWith<IllegalStateException> { lease.close() })

            jvmAndroidLifecycleLockReleaseForTest = null
            lease.close()
            acquireSqliteNowLifecycleLease(database.toString()).close()
        } finally {
            jvmAndroidLifecycleLockReleaseForTest = null
            database.deleteIfExists()
            database.resolveSibling("${database.fileName}.sqlitenow-opening-v1.lock").deleteIfExists()
        }
    }

    @Test
    fun descriptorFailureRetainsOwnershipAndRetriesOnlyTheDescriptorStage() = runTest {
        val database = Files.createTempFile("sqlitenow-lifecycle-file", ".sqlite3")
        val unlockCalls = AtomicInteger()
        val closeCalls = AtomicInteger()
        val descriptorFailure = IllegalStateException("LIFECYCLE_CLOSE_SENTINEL")
        val lease = acquireSqliteNowLifecycleLease(database.toString())
        try {
            jvmAndroidLifecycleLockReleaseForTest = { lock ->
                unlockCalls.incrementAndGet()
                lock.release()
            }
            jvmAndroidLifecycleFileCloseForTest = { file ->
                if (closeCalls.incrementAndGet() == 1) throw descriptorFailure
                file.close()
            }

            assertSame(descriptorFailure, assertFailsWith<IllegalStateException> { lease.close() })
            assertFailsWith<SqliteNowLifecycleBusyException> {
                acquireSqliteNowLifecycleLease(database.toString())
            }
            lease.close()
            assertTrue(unlockCalls.get() == 1 && closeCalls.get() == 2)
            acquireSqliteNowLifecycleLease(database.toString()).close()
        } finally {
            clearLifecycleHooks()
            database.deleteIfExists()
            database.resolveSibling("${database.fileName}.sqlitenow-opening-v1.lock").deleteIfExists()
        }
    }

    @Test
    fun concurrentRetrySerializesFailedUnlockAndDescriptorStages() = runTest {
        val database = Files.createTempFile("sqlitenow-lifecycle-concurrent", ".sqlite3")
        val unlockCalls = AtomicInteger()
        val closeCalls = AtomicInteger()
        val lease = acquireSqliteNowLifecycleLease(database.toString())
        try {
            jvmAndroidLifecycleLockReleaseForTest = { lock ->
                if (unlockCalls.incrementAndGet() == 1) throw IllegalStateException("unlock")
                lock.release()
            }
            jvmAndroidLifecycleFileCloseForTest = { file ->
                if (closeCalls.incrementAndGet() == 1) throw IllegalStateException("close")
                file.close()
            }
            assertFailsWith<IllegalStateException> { lease.close() }
            jvmAndroidLifecycleLockReleaseForTest = { lock ->
                unlockCalls.incrementAndGet()
                lock.release()
            }
            listOf(async { runCatching { lease.close() } }, async { runCatching { lease.close() } }).awaitAll()
            assertTrue(unlockCalls.get() == 2 && closeCalls.get() == 2)
            acquireSqliteNowLifecycleLease(database.toString()).close()
        } finally {
            clearLifecycleHooks()
            database.deleteIfExists()
            database.resolveSibling("${database.fileName}.sqlitenow-opening-v1.lock").deleteIfExists()
        }
    }

    @Test
    fun overlappingCloseCallersSharePhysicalAttemptAndFailure() = runTest {
        data class Scenario(val name: String, val failUnlock: Boolean, val failFile: Boolean)
        for (scenario in listOf(
            Scenario("unlock fails, descriptor completes", true, false),
            Scenario("unlock completes, descriptor fails", false, true),
            Scenario("both stages fail", true, true),
            Scenario("both stages complete", false, false),
        )) {
            val database = Files.createTempFile("sqlitenow-overlap", ".sqlite3")
            val lease = acquireSqliteNowLifecycleLease(database.toString())
            val executor = Executors.newFixedThreadPool(2)
            val entered = CountDownLatch(1)
            val joined = CountDownLatch(1)
            val release = CountDownLatch(1)
            val unlockCalls = AtomicInteger()
            val closeCalls = AtomicInteger()
            val unlockFailure = IllegalStateException("${scenario.name}: unlock")
            val fileFailure = IllegalStateException("${scenario.name}: descriptor")
            try {
                jvmAndroidLifecycleLockReleaseForTest = { lock ->
                    unlockCalls.incrementAndGet()
                    entered.countDown()
                    check(release.await(10, TimeUnit.SECONDS))
                    if (scenario.failUnlock) throw unlockFailure
                    lock.release()
                }
                jvmAndroidLifecycleFileCloseForTest = { file ->
                    closeCalls.incrementAndGet()
                    if (scenario.failFile) throw fileFailure
                    file.close()
                }
                jvmAndroidLifecycleCloseJoinedForTest = { joined.countDown() }
                val first = executor.submit<Throwable?> { runBlocking { runCatching { lease.close() }.exceptionOrNull() } }
                assertTrue(entered.await(10, TimeUnit.SECONDS), scenario.name)
                val second = executor.submit<Throwable?> { runBlocking { runCatching { lease.close() }.exceptionOrNull() } }
                assertTrue(joined.await(10, TimeUnit.SECONDS), scenario.name)
                assertFailsWith<SqliteNowLifecycleBusyException> {
                    acquireSqliteNowLifecycleLease(database.toString())
                }
                release.countDown()
                val expected = if (scenario.failUnlock) unlockFailure else if (scenario.failFile) fileFailure else null
                assertSame(expected, first.get(10, TimeUnit.SECONDS), scenario.name)
                assertSame(expected, second.get(10, TimeUnit.SECONDS), scenario.name)
                assertEquals(if (scenario.failUnlock && scenario.failFile) listOf(fileFailure) else emptyList(),
                    expected?.suppressedExceptions.orEmpty(), scenario.name)
                assertEquals(1, unlockCalls.get(), scenario.name)
                assertEquals(1, closeCalls.get(), scenario.name)
                if (scenario.failFile) assertFailsWith<SqliteNowLifecycleBusyException> {
                    acquireSqliteNowLifecycleLease(database.toString())
                }
                jvmAndroidLifecycleLockReleaseForTest = { lock -> unlockCalls.incrementAndGet(); lock.release() }
                jvmAndroidLifecycleFileCloseForTest = { file -> closeCalls.incrementAndGet(); file.close() }
                lease.close()
                assertEquals(if (scenario.failFile && scenario.failUnlock) 2 else 1, unlockCalls.get(), scenario.name)
                assertEquals(if (scenario.failFile) 2 else 1, closeCalls.get(), scenario.name)
                acquireSqliteNowLifecycleLease(database.toString()).close()
            } finally {
                release.countDown()
                executor.shutdown()
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS))
                clearLifecycleHooks()
                lease.close()
                database.deleteIfExists()
                database.resolveSibling("${database.fileName}.sqlitenow-opening-v1.lock").deleteIfExists()
            }
        }
    }

    private fun clearLifecycleHooks() {
        jvmAndroidLifecycleLockReleaseForTest = null
        jvmAndroidLifecycleFileCloseForTest = null
        jvmAndroidLifecycleCloseJoinedForTest = null
    }
}
