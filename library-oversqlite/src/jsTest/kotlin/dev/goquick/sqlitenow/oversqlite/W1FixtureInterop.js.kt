package dev.goquick.sqlitenow.oversqlite

import kotlin.js.Promise
import kotlinx.coroutines.await

@JsModule("./w1-test-fixtures/control.mjs")
@JsNonModule
private external object W1Control {
    fun workerModuleUrl(scenario: String): String
    fun configure(configuration: String)
    fun awaitHeld(): Promise<dynamic>
    fun releaseHeld()
    fun mark(phase: String)
    fun snapshot(): String
}

internal actual fun w1WorkerModuleUrl(scenario: String): String = W1Control.workerModuleUrl(scenario)
internal actual fun w1Configure(configuration: String) = W1Control.configure(configuration)
internal actual suspend fun w1AwaitHeld() { W1Control.awaitHeld().await() }
internal actual fun w1ReleaseHeld() = W1Control.releaseHeld()
internal actual fun w1Mark(phase: String) = W1Control.mark(phase)
internal actual fun w1Snapshot(): String = W1Control.snapshot()
