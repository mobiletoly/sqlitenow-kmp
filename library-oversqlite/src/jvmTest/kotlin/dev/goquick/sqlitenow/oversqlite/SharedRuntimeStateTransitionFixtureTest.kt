package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.JsonObject
import kotlin.io.path.createDirectories
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals

class SharedRuntimeStateTransitionFixtureTest : SharedRuntimeStateFixtureSupport() {
    private val fixtureFile = oversqliteContractFixture("runtime-state/transitions/basic.json")

    @Test
    fun kmpSharedRuntimeStateTransitionFixtureMatchesRuntime() = runBlocking {
        val spec = contractJson.decodeFromString<RuntimeStateTransitionSpec>(fixtureFile.readText())
        assertEquals(1, spec.formatVersion)
        val updatedCases = mutableListOf<RuntimeStateTransitionCase>()
        for (case in spec.cases) {
            updatedCases += runCase(case)
        }
        if (updateRuntimeStateExpected) {
            fixtureFile.parent.createDirectories()
            fixtureFile.writeText(
                contractJson.encodeToString(RuntimeStateTransitionSpec(formatVersion = 1, cases = updatedCases)) + "\n",
            )
        }
    }

    private suspend fun runCase(case: RuntimeStateTransitionCase): RuntimeStateTransitionCase {
        val updatedSteps = mutableListOf<RuntimeStateTransitionStep>()
        withRuntimeStateUsersDatabase {
            configureRuntimeStateServer(case.name, case.serverScript, server)
            server.start()
            for (step in case.steps) {
                val error = runStep(step)
                assertRuntimeStateExpectedException(case.name, step.expectedException, error)
                val actualState = dumpRuntimeState(db) as JsonObject
                if (updateRuntimeStateExpected) {
                    updatedSteps += step.copy(expectedState = actualState)
                } else {
                    val expected = step.expectedState ?: error("${case.name}/${step.action}: missing expectedState")
                    assertEquals(expected, actualState, "${case.name}/${step.action}: runtime state")
                    updatedSteps += step
                }
            }
        }
        return case.copy(steps = updatedSteps)
    }

    private suspend fun RuntimeStateUsersEnv.runStep(step: RuntimeStateTransitionStep): Throwable? {
        return when (step.action) {
            "open" -> client.open().exceptionOrNull()
            "attach" -> client.attach("user-1").exceptionOrNull()
            "localSql" -> runCatching { executeSetupSql(db, step.sql) }.exceptionOrNull()
            "pushPending" -> client.pushPending().exceptionOrNull()
            "pullToStable" -> client.pullToStable().exceptionOrNull()
            "reopen" -> runCatching {
                client.close()
                client = newRuntimeStateClient(db, http)
                client.open().getOrThrow()
                client.attach("user-1").getOrThrow()
            }.exceptionOrNull()
            "sourceInfo" -> client.sourceInfo().exceptionOrNull()
            else -> error("unknown runtime-state step action ${step.action}")
        }
    }
}
