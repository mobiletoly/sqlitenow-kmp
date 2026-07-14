package dev.goquick.sqlitenow.oversqlite

import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking

internal class SnapshotRestoreTimer(
    private val nanoTime: () -> Long = System::nanoTime,
) {
    var startedNanos: Long? = null
        private set
    var completedNanos: Long? = null
        private set

    val elapsedNanos: Long?
        get() {
            val started = startedNanos ?: return null
            val completed = completedNanos ?: return null
            return Math.subtractExact(completed, started)
        }

    suspend fun <T> measure(rebuild: suspend () -> T): T {
        check(startedNanos == null && completedNanos == null) {
            "snapshot restore timing can be recorded only once"
        }
        startedNanos = nanoTime()
        try {
            return rebuild()
        } finally {
            completedNanos = nanoTime()
        }
    }
}

class SnapshotMemoryHarnessCleanupTest {
    @Test
    fun restoreTimingMeasuresOnlyRebuildOnSuccessAndFailure() = runBlocking {
        val successEvents = mutableListOf<String>()
        val successTimes = ArrayDeque(listOf(100L, 140L))
        val successTimer = SnapshotRestoreTimer {
            successEvents += "clock"
            successTimes.removeFirst()
        }
        successEvents += "setup"
        val result = successTimer.measure {
            successEvents += "rebuild"
            "ok"
        }
        successEvents += "cleanup"

        assertEquals("ok", result)
        assertEquals(40L, successTimer.elapsedNanos)
        assertEquals(listOf("setup", "clock", "rebuild", "clock", "cleanup"), successEvents)

        val failureEvents = mutableListOf<String>()
        val failureTimes = ArrayDeque(listOf(200L, 260L))
        val failureTimer = SnapshotRestoreTimer {
            failureEvents += "clock"
            failureTimes.removeFirst()
        }
        failureEvents += "setup"
        val failure = assertFailsWith<IllegalStateException> {
            failureTimer.measure {
                failureEvents += "rebuild"
                error("rebuild failed")
            }
        }
        failureEvents += "cleanup"

        assertEquals("rebuild failed", failure.message)
        assertEquals(60L, failureTimer.elapsedNanos)
        assertEquals(listOf("setup", "clock", "rebuild", "clock", "cleanup"), failureEvents)
    }

    @Test
    fun cleanupAttemptsEveryActionAndSuppressesLaterFailures() {
        val calls = mutableListOf<String>()
        val failure = assertFailsWith<IllegalStateException> {
            runBlocking {
                withSnapshotMemoryCleanup {
                    defer(SnapshotMemoryCleanupPhase.CLIENT, "client") {
                        calls += "client"
                        error("client cleanup")
                    }
                    defer(SnapshotMemoryCleanupPhase.DATABASE, "database") {
                        calls += "database"
                    }
                    defer(SnapshotMemoryCleanupPhase.CLIENT_SAMPLER, "sampler") {
                        calls += "sampler"
                        error("sampler cleanup")
                    }
                    error("workload")
                }
            }
        }

        assertEquals("workload", failure.message)
        assertEquals(listOf("client", "database", "sampler"), calls)
        val cleanupFailure = failure.suppressed.single()
        assertEquals("snapshot memory cleanup failed: client", cleanupFailure.message)
        assertEquals("client cleanup", cleanupFailure.cause?.message)
        assertEquals(
            "snapshot memory cleanup failed: sampler",
            cleanupFailure.suppressed.single().message,
        )
    }

    @Test
    fun profileDirectoryDeletionCannotReachHistoricalSibling() {
        val sandbox = Files.createTempDirectory("snapshot-memory-owned-directory-test-")
        try {
            val historical = Files.createDirectory(
                sandbox.resolve("oversqlite-kmp-memory-baseline-historical"),
            )
            Files.writeString(historical.resolve("preserve.txt"), "preserve")
            val profile = SnapshotMemoryProfileDirectory.create(sandbox)
            Files.writeString(profile.path.resolve("profile.txt"), "delete")

            profile.deleteRecursively()

            assertFalse(Files.exists(profile.path))
            assertTrue(Files.isRegularFile(historical.resolve("preserve.txt")))
        } finally {
            deleteTestTree(sandbox)
        }
    }

    @Test
    fun workloadFailureStopsBothSamplersAndChildProcess() {
        val process = startShellProcess("exec /bin/sleep 30")
        lateinit var serverSampler: ProcessRssSampler
        lateinit var clientSampler: JvmMemorySampler
        try {
            val failure = assertFailsWith<IllegalStateException> {
                runBlocking {
                    withSnapshotMemoryCleanup {
                        defer(SnapshotMemoryCleanupPhase.FIXTURE, "test child") {
                            forceStopTestProcess(process)
                        }
                        serverSampler = ProcessRssSampler.start(process.pid())
                        defer(SnapshotMemoryCleanupPhase.SERVER_SAMPLER, "server sampler") {
                            serverSampler.stop()
                        }
                        clientSampler = JvmMemorySampler.start()
                        defer(SnapshotMemoryCleanupPhase.CLIENT_SAMPLER, "client sampler") {
                            clientSampler.stop()
                        }
                        error("injected workload failure")
                    }
                }
            }

            assertEquals("injected workload failure", failure.message)
            assertFalse(serverSampler.workerAliveForTest)
            assertFalse(clientSampler.workerAliveForTest)
            assertFalse(process.isAlive)
        } finally {
            forceStopTestProcess(process)
        }
    }

    @Test
    fun readinessFailuresForceProcessExitWithoutLeftovers() {
        data class Scenario(
            val name: String,
            val command: String,
            val readinessTimeoutMillis: Long,
        )

        val scenarios = listOf(
            Scenario(
                name = "malformed readiness",
                command = "printf 'not-ready\\n'; exec /bin/sleep 30",
                readinessTimeoutMillis = 1_000L,
            ),
            Scenario(
                name = "wrong PID",
                command = "printf 'snapshot_memory_server_ready port=1 pid=1\\n'; exec /bin/sleep 30",
                readinessTimeoutMillis = 1_000L,
            ),
            Scenario(
                name = "readiness timeout",
                command = "exec /bin/sleep 30",
                readinessTimeoutMillis = 100L,
            ),
        )

        scenarios.forEach { scenario ->
            val process = startShellProcess(scenario.command)
            try {
                assertFails(scenario.name) {
                    SnapshotMemoryFixtureProcess.fromStartedProcessForTest(
                        process = process,
                        readinessTimeoutMillis = scenario.readinessTimeoutMillis,
                        forceShutdownTimeoutMillis = 3_000L,
                    )
                }
                assertTrue(process.waitFor(3, TimeUnit.SECONDS), scenario.name)
                assertFalse(process.isAlive, scenario.name)
            } finally {
                forceStopTestProcess(process)
            }
        }
    }

    @Test
    fun shutdownFailuresForceProcessExitCloseOutputAndLeaveNoChild() {
        data class Scenario(
            val name: String,
            val shutdownRequester: (Int) -> Unit,
        )

        val scenarios = listOf(
            Scenario("shutdown request failure") { error("injected shutdown failure") },
            Scenario("graceful shutdown timeout") { },
        )
        val readyCommand =
            "printf 'snapshot_memory_server_ready port=1 pid=%s\\n' \"${'$'}${'$'}\"; " +
                "exec /bin/sleep 30"

        scenarios.forEach { scenario ->
            val process = startShellProcess(readyCommand)
            try {
                val fixture = SnapshotMemoryFixtureProcess.fromStartedProcessForTest(
                    process = process,
                    readinessTimeoutMillis = 1_000L,
                    gracefulShutdownTimeoutMillis = 100L,
                    forceShutdownTimeoutMillis = 3_000L,
                    shutdownRequester = scenario.shutdownRequester,
                )

                assertFails(scenario.name) { fixture.stop() }

                assertTrue(process.waitFor(3, TimeUnit.SECONDS), scenario.name)
                assertFalse(process.isAlive, scenario.name)
                assertTrue(fixture.outputClosed, scenario.name)
            } finally {
                forceStopTestProcess(process)
            }
        }
    }

    private fun startShellProcess(command: String): Process =
        ProcessBuilder("/bin/sh", "-c", command).redirectErrorStream(true).start()

    private fun forceStopTestProcess(process: Process) {
        if (process.isAlive) process.destroyForcibly()
        check(process.waitFor(3, TimeUnit.SECONDS)) { "test child process did not stop" }
        process.inputStream.close()
        process.errorStream.close()
        process.outputStream.close()
    }

    private fun deleteTestTree(path: Path) {
        if (!Files.exists(path)) return
        Files.walk(path).use { paths ->
            paths.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists)
        }
    }
}
