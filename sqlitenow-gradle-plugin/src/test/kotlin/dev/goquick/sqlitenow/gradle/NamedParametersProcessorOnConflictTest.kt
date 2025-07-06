package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.NamedParametersProcessor
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.insert.Insert
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class NamedParametersProcessorOnConflictTest {

    @Test
    fun testNamedParametersProcessorReplacesOnConflictParameters() {
        // Test the exact SQL from the user's example
        val sql = """
            INSERT INTO Person(email,
                                first_name,
                                last_name,
                                phone,
                                birth_date,
                                notes)
            VALUES (:email,
                                :firstName,
                                :lastName,
                                :phone,
                                :birthDate,
                                :notes)
            ON CONFLICT(email) DO UPDATE SET first_name = :firstName,
                                             last_name = :lastName,
                                             phone   = :phone,
                                             birth_date = :birthDate,
                                             notes   = :notes2;
        """.trimIndent()
        
        // Parse the SQL
        val statement = CCJSqlParserUtil.parse(sql) as Insert
        
        // Process with NamedParametersProcessor
        val processor = NamedParametersProcessor(statement)
        

        
        // Verify that all named parameters are replaced with ?
        assertFalse(processor.processedSql.contains(":email"), "Processed SQL should not contain :email")
        assertFalse(processor.processedSql.contains(":firstName"), "Processed SQL should not contain :firstName")
        assertFalse(processor.processedSql.contains(":lastName"), "Processed SQL should not contain :lastName")
        assertFalse(processor.processedSql.contains(":phone"), "Processed SQL should not contain :phone")
        assertFalse(processor.processedSql.contains(":birthDate"), "Processed SQL should not contain :birthDate")
        assertFalse(processor.processedSql.contains(":notes"), "Processed SQL should not contain :notes")
        assertFalse(processor.processedSql.contains(":notes2"), "Processed SQL should not contain :notes2")
        
        // Verify that the processed SQL doesn't have extra ? marks
        val questionMarkCount = processor.processedSql.count { it == '?' }
        assertEquals(processor.parameters.size, questionMarkCount, 
            "Number of ? placeholders should match number of parameters")
        
        // Verify correct parameter order
        val expectedParameters = listOf(
            "email", "firstName", "lastName", "phone", "birthDate", "notes",  // VALUES clause
            "firstName", "lastName", "phone", "birthDate", "notes2"           // UPDATE clause
        )
        assertEquals(expectedParameters, processor.parameters, 
            "Parameters should be in correct order including duplicates")
        
        // Verify the processed SQL structure is correct
        val processedSqlNormalized = processor.processedSql.replace("\\s+".toRegex(), " ").trim()
        
        // Should contain the basic structure
        assert(processedSqlNormalized.contains("INSERT INTO Person")) { "Should contain INSERT INTO Person" }
        assert(processedSqlNormalized.contains("VALUES (?, ?, ?, ?, ?, ?)")) { "Should contain VALUES with 6 ? placeholders" }
        assert(processedSqlNormalized.contains("ON CONFLICT")) { "Should contain ON CONFLICT" }
        assert(processedSqlNormalized.contains("DO UPDATE SET")) { "Should contain DO UPDATE SET" }
        
        // The UPDATE SET clause should have ? placeholders, not named parameters
        val updateSetPart = processedSqlNormalized.substringAfter("DO UPDATE SET")
        assertFalse(updateSetPart.contains(":"), "UPDATE SET clause should not contain any : parameters")
        
        // Should contain 5 more ? placeholders in the UPDATE clause
        val updateQuestionMarks = updateSetPart.count { it == '?' }
        assertEquals(5, updateQuestionMarks, "UPDATE clause should have 5 ? placeholders")
    }

    @Test
    fun testNamedParametersProcessorWithSimpleOnConflict() {
        // Test a simpler case to isolate the issue
        val sql = """
            INSERT INTO Person(name, email) 
            VALUES (:name, :email) 
            ON CONFLICT(email) DO UPDATE SET name = :updatedName
        """.trimIndent()
        
        val statement = CCJSqlParserUtil.parse(sql) as Insert
        val processor = NamedParametersProcessor(statement)
        

        
        // Should not contain any named parameters
        assertFalse(processor.processedSql.contains(":name"), "Should not contain :name")
        assertFalse(processor.processedSql.contains(":email"), "Should not contain :email")
        assertFalse(processor.processedSql.contains(":updatedName"), "Should not contain :updatedName")
        
        // Should have exactly 3 parameters and 3 ? placeholders
        assertEquals(listOf("name", "email", "updatedName"), processor.parameters)
        assertEquals(3, processor.processedSql.count { it == '?' })
    }

    @Test
    fun testNamedParametersProcessorWithDuplicateParameters() {
        // Test case with duplicate parameter names
        val sql = """
            INSERT INTO Person(name, email) 
            VALUES (:name, :email) 
            ON CONFLICT(email) DO UPDATE SET name = :name, email = :email
        """.trimIndent()
        
        val statement = CCJSqlParserUtil.parse(sql) as Insert
        val processor = NamedParametersProcessor(statement)
        

        
        // Should not contain any named parameters
        assertFalse(processor.processedSql.contains(":name"), "Should not contain :name")
        assertFalse(processor.processedSql.contains(":email"), "Should not contain :email")
        
        // Should have 4 parameters (2 duplicates) and 4 ? placeholders
        assertEquals(listOf("name", "email", "name", "email"), processor.parameters)
        assertEquals(4, processor.processedSql.count { it == '?' })
    }

    @Test
    fun testNamedParametersProcessorWithMixedLiteralsAndParameters() {
        // Test the exact SQL from the user's example with mixed literals and parameters
        val sql = """
            INSERT INTO Person(email,
                                first_name,
                                last_name,
                                phone,
                                birth_date,
                                notes)
            VALUES ('hello@world.com',
                                :firstName,
                                :lastName,
                                :phone,
                                :birthDate,
                                :notes)
            ON CONFLICT(email) DO UPDATE SET first_name = :firstName,
                                             last_name = :lastName,
                                             phone   = '333-444-5555',
                                             birth_date = :birthDate2,
                                             notes   = :notes;
        """.trimIndent()

        // Parse the SQL
        val statement = CCJSqlParserUtil.parse(sql) as Insert

        // Process with NamedParametersProcessor
        val processor = NamedParametersProcessor(statement)



        // Verify that all named parameters are replaced with ?
        assertFalse(processor.processedSql.contains(":firstName"), "Processed SQL should not contain :firstName")
        assertFalse(processor.processedSql.contains(":lastName"), "Processed SQL should not contain :lastName")
        assertFalse(processor.processedSql.contains(":phone"), "Processed SQL should not contain :phone")
        assertFalse(processor.processedSql.contains(":birthDate"), "Processed SQL should not contain :birthDate")
        assertFalse(processor.processedSql.contains(":birthDate2"), "Processed SQL should not contain :birthDate2")
        assertFalse(processor.processedSql.contains(":notes"), "Processed SQL should not contain :notes")

        // Verify that string literals are preserved
        assert(processor.processedSql.contains("'hello@world.com'")) { "Should preserve 'hello@world.com' literal" }
        assert(processor.processedSql.contains("'333-444-5555'")) { "Should preserve '333-444-5555' literal" }

        // Verify correct parameter order
        val expectedParameters = listOf(
            "firstName", "lastName", "phone", "birthDate", "notes",  // VALUES clause (excluding literal)
            "firstName", "lastName", "birthDate2", "notes"           // UPDATE clause (excluding literal)
        )
        assertEquals(expectedParameters, processor.parameters,
            "Parameters should be in correct order excluding literals")

        // Verify the processed SQL structure is correct
        val processedSqlNormalized = processor.processedSql.replace("\\s+".toRegex(), " ").trim()

        // Should contain the basic structure with literals preserved
        assert(processedSqlNormalized.contains("VALUES ('hello@world.com', ?, ?, ?, ?, ?)")) {
            "VALUES clause should have literal and ? placeholders"
        }
        assert(processedSqlNormalized.contains("phone = '333-444-5555'")) {
            "UPDATE clause should preserve phone literal"
        }

        // Count ? placeholders - should match number of parameters
        val questionMarkCount = processor.processedSql.count { it == '?' }
        assertEquals(processor.parameters.size, questionMarkCount,
            "Number of ? placeholders should match number of parameters")
    }
}
