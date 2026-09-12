
package dev.goquick.sqlitenow.core.worker

import kotlin.js.Promise
import kotlinx.coroutines.await

@JsModule("./bound-write-test-fixtures/bound-write-runner.mjs")
@JsNonModule
private external object BoundWriteFixtureModule {
    fun runBoundWriteFixture(scenario: String): Promise<String>
}

internal actual suspend fun runBoundWriteFaultFixture(scenario: String): String =
    BoundWriteFixtureModule.runBoundWriteFixture(scenario).await()
