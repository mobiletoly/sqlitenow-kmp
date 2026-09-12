package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SharedOpeningScenarioMatrixTest {
    @Test
    fun everySharedScenarioOwnsRealKmpBehaviorTests() {
        val fixture = Json.parseToJsonElement(
            oversqliteContractFixture("opening-state/scenarios/v1.json").toFile().readText(),
        ).jsonObject
        assertEquals("sqlitenow-opening-scenarios/v1", fixture.getValue("format").jsonPrimitive.content)

        val scenarios = fixture.getValue("scenarios").jsonArray
        assertEquals(
            setOf(
                "fresh",
                "unchanged",
                "local_only",
                "generation_change",
                "malformed_admission",
                "pending_admission",
                "handwritten_escape",
                "generated_escape",
            ),
            scenarios.map { it.jsonObject.getValue("id").jsonPrimitive.content }.toSet(),
        )

        val root = oversqliteContractFixture("").parent
        scenarios.forEach { element ->
            val scenario = element.jsonObject
            assertTrue(scenario.getValue("required_observations").jsonArray.isNotEmpty())
            val owner = scenario.getValue("kmp").jsonObject
            val source = root.resolve(owner.getValue("source").jsonPrimitive.content)
            val test = owner.getValue("test").jsonPrimitive.content
            assertTrue(Files.isRegularFile(source), "missing KMP owner source $source")
            assertTrue(source.toFile().readText().contains(test), "missing KMP owner test $test")
        }
    }
}
