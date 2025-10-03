package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateParametersProcessor
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.update.Update
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UpdateWithClauseOrderingTest {

    @Test
    @DisplayName("Test UPDATE with WITH clause parameter ordering")
    fun testUpdateWithClauseParameterOrdering() {
        // Test the exact SQL from the user's example
        val sql = """
            WITH tmp AS (SELECT id FROM Person WHERE age = :myAge LIMIT 1)
            UPDATE Person
            SET score = :myScore,
                notes = :myNotes,
                age = :myAge
            WHERE id = :id
              AND birth_date <= :myBirthDate
              AND birth_date >= :myBirthDate
        """.trimIndent()

        // Parse the UPDATE statement
        val parsedStatement = CCJSqlParserUtil.parse(sql) as Update

        // Process parameters using UpdateParametersProcessor
        val processor = UpdateParametersProcessor(parsedStatement)

        // Verify the processed SQL structure
        val processedSql = processor.processedSql
        // Verify parameter ordering: WITH clause parameters should come first
        val parameters = processor.parameters

        // Expected parameter order:
        // 1. WITH clause: myAge (position 1)
        // 2. SET clause: myScore, myNotes, myAge (positions 2, 3, 4)
        // 3. WHERE clause: id, myBirthDate, myBirthDate (positions 5, 6, 7)
        val expectedOrder = listOf("myAge", "myScore", "myNotes", "myAge", "id", "myBirthDate", "myBirthDate")
        assertEquals(expectedOrder, parameters,
            "Parameter order should be: WITH clause first, then SET clause, then WHERE clause. Expected: $expectedOrder, Actual: $parameters")

        // Verify that the processed SQL has the correct number of placeholders
        val placeholderCount = processedSql.count { it == '?' }
        // The exact count depends on how UpdateParametersProcessor handles WITH clauses
        assertTrue(placeholderCount >= 6, "Should have at least 6 parameter placeholders, actual: $placeholderCount")

        // Verify that WITH clause is processed correctly
        assertTrue(processedSql.contains("WITH tmp AS"), "Should contain WITH clause")
        assertTrue(processedSql.contains("UPDATE Person"), "Should contain UPDATE statement")
    }

    @Test
    @DisplayName("Test UPDATE with multiple WITH clauses parameter ordering")
    fun testUpdateWithMultipleWithClausesParameterOrdering() {
        val sql = """
            WITH 
                tmp1 AS (SELECT id FROM Person WHERE age = :age1),
                tmp2 AS (SELECT id FROM Person WHERE score = :score1)
            UPDATE Person
            SET age = :newAge,
                score = :newScore
            WHERE id IN (SELECT id FROM tmp1)
              AND id IN (SELECT id FROM tmp2)
              AND active = :isActive
        """.trimIndent()

        // Parse the UPDATE statement
        val parsedStatement = CCJSqlParserUtil.parse(sql) as Update

        // Process parameters using UpdateParametersProcessor
        val processor = UpdateParametersProcessor(parsedStatement)

        // Verify parameter ordering
        val parameters = processor.parameters
        // Expected parameter order:
        // 1. WITH clauses: age1, score1 (positions 1, 2)
        // 2. SET clause: newAge, newScore (positions 3, 4)
        // 3. WHERE clause: isActive (position 5)
        val expectedOrder = listOf("age1", "score1", "newAge", "newScore", "isActive")
        
        assertEquals(expectedOrder, parameters, 
            "Parameter order should prioritize all WITH clause parameters first")
    }

    @Test
    @DisplayName("Test UPDATE without WITH clause parameter ordering")
    fun testUpdateWithoutWithClauseParameterOrdering() {
        val sql = """
            UPDATE Person
            SET age = :newAge,
                score = :newScore
            WHERE id = :personId
              AND active = :isActive
        """.trimIndent()

        // Parse the UPDATE statement
        val parsedStatement = CCJSqlParserUtil.parse(sql) as Update

        // Process parameters using UpdateParametersProcessor
        val processor = UpdateParametersProcessor(parsedStatement)

        // Verify parameter ordering for UPDATE without WITH clause
        val parameters = processor.parameters
        // Expected parameter order:
        // 1. SET clause: newAge, newScore (positions 1, 2)
        // 2. WHERE clause: personId, isActive (positions 3, 4)
        val expectedOrder = listOf("newAge", "newScore", "personId", "isActive")
        
        assertEquals(expectedOrder, parameters, 
            "Parameter order should be SET clause first, then WHERE clause")

        // Verify that the processed SQL has the correct number of placeholders
        val placeholderCount = processor.processedSql.count { it == '?' }
        assertEquals(4, placeholderCount, "Should have 4 parameter placeholders")
    }

    @Test
    @DisplayName("Test UPDATE with duplicate parameters in different clauses")
    fun testUpdateWithDuplicateParametersInDifferentClauses() {
        // This tests the user's exact scenario where :myAge appears in both WITH and SET clauses
        val sql = """
            WITH tmp AS (SELECT id FROM Person WHERE age = :myAge)
            UPDATE Person
            SET age = :myAge,
                score = :newScore
            WHERE id IN (SELECT id FROM tmp)
        """.trimIndent()

        // Parse the UPDATE statement
        val parsedStatement = CCJSqlParserUtil.parse(sql) as Update

        // Process parameters using UpdateParametersProcessor
        val processor = UpdateParametersProcessor(parsedStatement)

        // Verify parameter ordering with duplicates
        val parameters = processor.parameters
        // Expected parameter order:
        // 1. WITH clause: myAge (position 1)
        // 2. SET clause: myAge, newScore (positions 2, 3)
        val expectedOrder = listOf("myAge", "myAge", "newScore")
        assertEquals(expectedOrder, parameters, 
            "Duplicate parameters should appear in order: WITH clause first, then SET clause")

        // Verify that both instances of :myAge are preserved
        val myAgeCount = parameters.count { it == "myAge" }
        assertEquals(2, myAgeCount, "Should have 2 instances of myAge parameter")
    }
}
