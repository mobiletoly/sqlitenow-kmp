package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateParametersProcessor
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.update.Update
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UpdateUserScenarioTest {

    @Test
    @DisplayName("Test user's exact UPDATE scenario with WITH clause parameter ordering")
    fun testUserExactUpdateScenario() {
        // User's exact SQL from the issue description
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
        // Verify parameter ordering matches user's expected order
        val parameters = processor.parameters

        // Expected parameter order based on user's requirements:
        // 1. WITH clause parameters first: myAge (position 1)
        // 2. SET clause parameters: myScore, myNotes, myAge (positions 2, 3, 4)
        // 3. WHERE clause parameters: id, myBirthDate, myBirthDate (positions 5, 6, 7)
        val expectedOrder = listOf("myAge", "myScore", "myNotes", "myAge", "id", "myBirthDate", "myBirthDate")
        
        assertEquals(expectedOrder, parameters, 
            "Parameter order should match user's expected order for data class generation and function parameters")

        // Verify that WITH clause parameters come first
        assertTrue(parameters[0] == "myAge", "First parameter should be myAge from WITH clause")
        
        // Verify that SET clause parameters come next
        assertTrue(parameters[1] == "myScore", "Second parameter should be myScore from SET clause")
        assertTrue(parameters[2] == "myNotes", "Third parameter should be myNotes from SET clause")
        assertTrue(parameters[3] == "myAge", "Fourth parameter should be myAge from SET clause")
        
        // Verify that WHERE clause parameters come last
        assertTrue(parameters[4] == "id", "Fifth parameter should be id from WHERE clause")
        assertTrue(parameters[5] == "myBirthDate", "Sixth parameter should be myBirthDate from WHERE clause")
        assertTrue(parameters[6] == "myBirthDate", "Seventh parameter should be myBirthDate from WHERE clause")

        // Verify that the processed SQL has proper placeholders
        val placeholderCount = processedSql.count { it == '?' }
        assertTrue(placeholderCount >= 6, "Should have at least 6 parameter placeholders")

        // Verify SQL structure is preserved
        assertTrue(processedSql.contains("WITH tmp AS"), "Should preserve WITH clause structure")
        assertTrue(processedSql.contains("UPDATE Person"), "Should preserve UPDATE statement")
        assertTrue(processedSql.contains("SET score = ?"), "Should have placeholder for score")
        assertTrue(processedSql.contains("notes = ?"), "Should have placeholder for notes")
        assertTrue(processedSql.contains("age = ?"), "Should have placeholder for age in SET clause")
        assertTrue(processedSql.contains("WHERE id = ?"), "Should have placeholder for id in WHERE clause")
        assertTrue(processedSql.contains("birth_date <= ?"), "Should have placeholder for birth_date comparison")
        assertTrue(processedSql.contains("birth_date >= ?"), "Should have placeholder for birth_date comparison")
    }

    @Test
    @DisplayName("Test that UPDATE parameter ordering enables correct data class generation")
    fun testUpdateParameterOrderingForDataClassGeneration() {
        // This test verifies that the parameter ordering will result in the correct data class
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

        val parsedStatement = CCJSqlParserUtil.parse(sql) as Update
        val processor = UpdateParametersProcessor(parsedStatement)
        val parameters = processor.parameters

        // Verify that the parameter order will generate the expected data class structure:
        // public data class UpdateById(
        //     public val myAge: Long?,        // Position 1 (WITH clause)
        //     public val myScore: Long,       // Position 2 (SET clause)
        //     public val myNotes: PersonNote?, // Position 3 (SET clause)
        //     public val myAge: Long?,        // Position 4 (SET clause - duplicate)
        //     public val id: Long,            // Position 5 (WHERE clause)
        //     public val myBirthDate: LocalDate?, // Position 6 (WHERE clause)
        //     public val myBirthDate: LocalDate?, // Position 7 (WHERE clause - duplicate)
        // )

        // Note: In the actual data class, duplicates will be deduplicated, but the order is important
        // for the function parameter binding order

        assertEquals(7, parameters.size, "Should have 7 parameters total")
        
        // Verify WITH clause parameter comes first
        assertEquals("myAge", parameters[0], "WITH clause parameter should be first")
        
        // Verify SET clause parameters come next (positions 2-4)
        assertEquals("myScore", parameters[1], "SET clause myScore should be second")
        assertEquals("myNotes", parameters[2], "SET clause myNotes should be third")
        assertEquals("myAge", parameters[3], "SET clause myAge should be fourth")
        
        // Verify WHERE clause parameters come last (positions 5-7)
        assertEquals("id", parameters[4], "WHERE clause id should be fifth")
        assertEquals("myBirthDate", parameters[5], "WHERE clause myBirthDate should be sixth")
        assertEquals("myBirthDate", parameters[6], "WHERE clause myBirthDate should be seventh")

        // This ordering ensures that:
        // 1. WITH clause parameters are bound first in the generated function
        // 2. SET clause parameters are bound next
        // 3. WHERE clause parameters are bound last
        // This matches the user's expected function signature and parameter binding order
    }
}
