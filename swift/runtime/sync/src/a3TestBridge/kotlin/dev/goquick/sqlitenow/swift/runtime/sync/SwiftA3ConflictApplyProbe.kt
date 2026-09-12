package dev.goquick.sqlitenow.swift.runtime.sync

import dev.goquick.sqlitenow.oversqlite.DefaultOversqliteClient
import dev.goquick.sqlitenow.oversqlite.OversqliteClient
import dev.goquick.sqlitenow.oversqlite.SnapshotApplyFaultInjector
import kotlinx.coroutines.CompletableDeferred
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.experimental.ExperimentalNativeApi
import kotlin.native.Platform

// Only the opt-in XCTest framework compiles this file. The ordinary framework
// compiles a no-op implementation and has no C probe or friend-module access.
@OptIn(ExperimentalAtomicApi::class)
internal object SwiftA3ConflictApplyProbe {
    private class Gate(val requestHash: String) {
        val release = CompletableDeferred<Unit>()
    }

    private val armed = AtomicReference<Gate?>(null)
    private val entered = AtomicBoolean(false)

    fun arm(requestHash: String): Int {
        if (requestHash.length != 64 || requestHash.any { it !in '0'..'9' && it !in 'a'..'f' }) return 0
        if (!armed.compareAndSet(null, Gate(requestHash))) return 0
        entered.store(false)
        return 1
    }

    fun entered(): Int = if (entered.load()) 1 else 0

    fun release(): Int {
        val gate = armed.exchange(null) ?: return 0
        gate.release.complete(Unit)
        return 1
    }

    suspend fun <T> aroundResolution(
        requestHash: String,
        client: OversqliteClient,
        operation: suspend () -> T,
    ): T {
        val gate = armed.load()?.takeIf { it.requestHash == requestHash } ?: return operation()
        val owner = checkNotNull(client as? DefaultOversqliteClient)
        owner.setSnapshotApplyFaultInjectorForTest(
            SnapshotApplyFaultInjector(beforeCommit = {
                entered.store(true)
                gate.release.await()
            }),
        )
        return try {
            operation()
        } finally {
            owner.setSnapshotApplyFaultInjectorForTest(null)
        }
    }
}

@OptIn(ExperimentalNativeApi::class)
public fun armA3ConflictApplyForSwiftTest(requestHash: String): Int =
    if (Platform.isDebugBinary) SwiftA3ConflictApplyProbe.arm(requestHash) else 0

@OptIn(ExperimentalNativeApi::class)
public fun a3ConflictApplyEnteredForSwiftTest(): Int =
    if (Platform.isDebugBinary) SwiftA3ConflictApplyProbe.entered() else 0

@OptIn(ExperimentalNativeApi::class)
public fun releaseA3ConflictApplyForSwiftTest(): Int =
    if (Platform.isDebugBinary) SwiftA3ConflictApplyProbe.release() else 0
