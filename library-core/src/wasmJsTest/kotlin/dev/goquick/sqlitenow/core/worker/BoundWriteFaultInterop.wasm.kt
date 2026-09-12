@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.core.worker

import kotlin.js.JsAny
import kotlin.js.Promise
import kotlinx.coroutines.await

@JsModule("./bound-write-test-fixtures/bound-write-runner.mjs")
private external object BoundWriteFixtureModule {
    fun runBoundWriteFixture(scenario: String): Promise<JsAny>
}

@JsFun("(value) => String(value)")
private external fun fixtureString(value: JsAny): String

internal actual suspend fun runBoundWriteFaultFixture(scenario: String): String =
    fixtureString(BoundWriteFixtureModule.runBoundWriteFixture(scenario).await())
