package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateParametersProcessor
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.update.Update
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UpdateWithClauseOrderingTest {

    @TestFactory
    fun `update parameter ordering cases`(): List<DynamicTest> =
        listOf(
            UpdateParameterOrderingCase(
                displayName = "WITH clause parameters come before SET and WHERE parameters",
                sql = """
                    WITH tmp AS (SELECT id FROM Person WHERE age = :myAge LIMIT 1)
                    UPDATE Person
                    SET score = :myScore,
                        notes = :myNotes,
                        age = :myAge
                    WHERE id = :id
                      AND birth_date <= :myBirthDate
                      AND birth_date >= :myBirthDate
                """.trimIndent(),
                expectedOrder = listOf("myAge", "myScore", "myNotes", "myAge", "id", "myBirthDate", "myBirthDate"),
                minimumPlaceholderCount = 6,
                expectedSqlFragments = listOf("WITH tmp AS", "UPDATE Person"),
            ),
            UpdateParameterOrderingCase(
                displayName = "multiple WITH clause parameters stay ahead of SET and WHERE parameters",
                sql = """
                    WITH 
                        tmp1 AS (SELECT id FROM Person WHERE age = :age1),
                        tmp2 AS (SELECT id FROM Person WHERE score = :score1)
                    UPDATE Person
                    SET age = :newAge,
                        score = :newScore
                    WHERE id IN (SELECT id FROM tmp1)
                      AND id IN (SELECT id FROM tmp2)
                      AND active = :isActive
                """.trimIndent(),
                expectedOrder = listOf("age1", "score1", "newAge", "newScore", "isActive"),
            ),
            UpdateParameterOrderingCase(
                displayName = "UPDATE without WITH uses SET parameters before WHERE parameters",
                sql = """
                    UPDATE Person
                    SET age = :newAge,
                        score = :newScore
                    WHERE id = :personId
                      AND active = :isActive
                """.trimIndent(),
                expectedOrder = listOf("newAge", "newScore", "personId", "isActive"),
                expectedPlaceholderCount = 4,
            ),
            UpdateParameterOrderingCase(
                displayName = "duplicate parameters are preserved in clause order",
                sql = """
                    WITH tmp AS (SELECT id FROM Person WHERE age = :myAge)
                    UPDATE Person
                    SET age = :myAge,
                        score = :newScore
                    WHERE id IN (SELECT id FROM tmp)
                """.trimIndent(),
                expectedOrder = listOf("myAge", "myAge", "newScore"),
                expectedParameterCounts = mapOf("myAge" to 2),
            ),
        ).map { case ->
            DynamicTest.dynamicTest(case.displayName) {
                assertUpdateParameterOrdering(case)
            }
        }

    private fun assertUpdateParameterOrdering(case: UpdateParameterOrderingCase) {
        val parsedStatement = CCJSqlParserUtil.parse(case.sql) as Update
        val processor = UpdateParametersProcessor(parsedStatement)

        assertEquals(case.expectedOrder, processor.parameters)

        val placeholderCount = processor.processedSql.count { it == '?' }
        case.minimumPlaceholderCount?.let { expectedMinimum ->
            assertTrue(
                placeholderCount >= expectedMinimum,
                "Should have at least $expectedMinimum parameter placeholders, actual: $placeholderCount"
            )
        }
        case.expectedPlaceholderCount?.let { expectedCount ->
            assertEquals(expectedCount, placeholderCount)
        }
        case.expectedSqlFragments.forEach { fragment ->
            assertTrue(processor.processedSql.contains(fragment), "Should contain $fragment")
        }
        case.expectedParameterCounts.forEach { (parameter, expectedCount) ->
            assertEquals(expectedCount, processor.parameters.count { it == parameter })
        }
    }

    private data class UpdateParameterOrderingCase(
        val displayName: String,
        val sql: String,
        val expectedOrder: List<String>,
        val minimumPlaceholderCount: Int? = null,
        val expectedPlaceholderCount: Int? = null,
        val expectedSqlFragments: List<String> = emptyList(),
        val expectedParameterCounts: Map<String, Int> = emptyMap(),
    )
}
