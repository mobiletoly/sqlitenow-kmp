package dev.goquick.sqlitenow.oversqlite

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertIs

internal class SnapshotProcessMemoryObservationTest {
    @Test fun timeoutKillsAndReapsOwnedChild() {
        val output = Files.createTempFile("snapshot-memory-timeout", ".txt")
        try {
            val result = runMemoryObservationCommand(child("wait"), output, timeoutMillis = 500)
            assertTrue(result.timedOut)
            assertFalse(ProcessHandle.of(result.pid).map { it.isAlive }.orElse(false), "timed-out diagnostic child must be reaped")
        } finally { Files.deleteIfExists(output) }
    }

    @Test fun nonzeroExitIsRecordedWithoutDiscardingDiagnosticOutput() {
        val output = Files.createTempFile("snapshot-memory-command", ".txt")
        try {
            val result = runMemoryObservationCommand(child("fail"), output)
            assertFalse(result.timedOut)
            assertEquals(7, result.exit)
            assertTrue(Files.readString(output).contains("retained diagnostic"))
            assertFalse(ProcessHandle.of(result.pid).map { it.isAlive }.orElse(false))
        } finally { Files.deleteIfExists(output) }
    }

    @Test fun disabledOrAbsentNativeMemoryTrackingCannotQualifyObservation() {
        assertFalse(hasEnabledNativeMemoryTracking("Native memory tracking is not enabled"))
        assertFalse(hasEnabledNativeMemoryTracking(""))
        assertTrue(hasEnabledNativeMemoryTracking("Native Memory Tracking:\nTotal: reserved=128KB, committed=64KB"))
    }

    @Test fun closureOnlyModeDoesNotRunToolsDuringInstallation() {
        val root = Files.createTempDirectory("snapshot-memory-closure-mode")
        val output = root.resolve("observations")
        try {
            val observer = SnapshotProcessMemoryObservation(output, closureOnly = true)
            for (phase in listOf("before_setup", "before_initial_sync", "initial_stage_complete", "after_initial_sync", "before_rebuild", "after_rebuild"))
                observer.capture(phase)
            assertFalse(Files.exists(output), "closure-only mode must not start command/file observations in the hot path")
        } finally { Files.deleteIfExists(root) }
    }

    @Test fun interruptedWaitPreservesPrimaryFailureAndReapsChild() {
        val output = Files.createTempFile("snapshot-memory-interruption", ".txt")
        val failure = AtomicReference<Throwable?>()
        val worker = Thread {
            try { runMemoryObservationCommand(child("wait"), output, timeoutMillis = 10_000) }
            catch (error: Throwable) { failure.set(error) }
        }
        try {
            worker.start()
            val deadline = System.nanoTime() + 5_000_000_000L
            while (Files.size(output) == 0L && worker.isAlive && System.nanoTime() < deadline) Thread.sleep(20)
            assertTrue(Files.size(output) > 0L, "owned child must report its PID")
            val pid = Files.readString(output).trim().toLong()
            worker.interrupt(); worker.join(5_000)
            assertFalse(worker.isAlive)
            assertIs<InterruptedException>(failure.get())
            assertFalse(ProcessHandle.of(pid).map { it.isAlive }.orElse(false))
        } finally {
            worker.interrupt(); worker.join(5_000)
            Files.deleteIfExists(output)
        }
    }

    private fun child(mode: String): List<String> = listOf(
        Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
        requireNotNull(System.getProperty("coordinatedopening.jvmTest.classpath")),
        CommandFixture::class.java.name, mode,
    )

    object CommandFixture {
        @JvmStatic fun main(args: Array<String>) {
            if (args.single() == "wait") { println(ProcessHandle.current().pid()); Thread.sleep(30_000) }
            else { println("retained diagnostic"); kotlin.system.exitProcess(7) }
        }
    }
}
