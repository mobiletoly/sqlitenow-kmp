package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.InsertStatement
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.insert.Insert
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class InsertOnConflictParameterOrderTest {

    @Test
    fun testInsertOnConflictParameterOrder() {
        // Test SQL with duplicate parameter names in VALUES and UPDATE clauses
        val sql = """
            INSERT INTO Person(email, first_name, last_name, phone, birth_date, notes)
            VALUES (:email, :firstName, :lastName, :phone, :birthDate, :notes)
            ON CONFLICT(email) DO UPDATE SET 
                first_name = :firstName,
                last_name = :lastName,
                phone = :phone,
                birth_date = :birthDate,
                notes = :notes2
        """.trimIndent()
        
        // Parse the SQL
        val statement = CCJSqlParserUtil.parse(sql) as Insert
        
        // Create an in-memory SQLite connection for testing
        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        
        // Parse the InsertStatement
        val insertStatement = InsertStatement.parse(statement, conn)
        
        // Verify that parameters are captured in the correct order (including duplicates)
        val expectedParameterOrder = listOf(
            "email",      // VALUES clause position 1
            "firstName",  // VALUES clause position 2
            "lastName",   // VALUES clause position 3
            "phone",      // VALUES clause position 4
            "birthDate",  // VALUES clause position 5
            "notes",      // VALUES clause position 6
            "firstName",  // UPDATE clause position 7 (duplicate)
            "lastName",   // UPDATE clause position 8 (duplicate)
            "phone",      // UPDATE clause position 9 (duplicate)
            "birthDate",  // UPDATE clause position 10 (duplicate)
            "notes2"      // UPDATE clause position 11 (different parameter)
        )

        assertEquals(expectedParameterOrder, insertStatement.namedParameters,
            "Parameters should be captured in the correct order including duplicates")
        
        // Verify column mappings
        val columnMappings = insertStatement.columnNamesAssociatedWithNamedParameters
        assertEquals("email", columnMappings["email"])
        assertEquals("first_name", columnMappings["firstName"])
        assertEquals("last_name", columnMappings["lastName"])
        assertEquals("phone", columnMappings["phone"])
        assertEquals("birth_date", columnMappings["birthDate"])
        assertEquals("notes", columnMappings["notes"])
        assertEquals("notes", columnMappings["notes2"]) // notes2 maps to notes column
        
        conn.close()
    }

    @Test
    fun testInsertOnConflictWithSameParameterNames() {
        // Test case where all UPDATE parameters have the same names as VALUES parameters
        val sql = """
            INSERT INTO UserProfile(id, isMetric, nickname, height, birthday, gender)
            VALUES ('main', :isMetric, :nickname, :height, :birthday, :gender)
            ON CONFLICT(id) DO UPDATE SET 
                isMetric = :isMetric,
                nickname = :nickname,
                height = :height,
                birthday = :birthday,
                gender = :gender
        """.trimIndent()
        
        val statement = CCJSqlParserUtil.parse(sql) as Insert
        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        val insertStatement = InsertStatement.parse(statement, conn)
        
        // Should have 10 parameters total (5 from VALUES + 5 from UPDATE)
        val expectedParameterOrder = listOf(
            "isMetric",   // VALUES position 2
            "nickname",   // VALUES position 3
            "height",     // VALUES position 4
            "birthday",   // VALUES position 5
            "gender",     // VALUES position 6
            "isMetric",   // UPDATE position 7 (duplicate)
            "nickname",   // UPDATE position 8 (duplicate)
            "height",     // UPDATE position 9 (duplicate)
            "birthday",   // UPDATE position 10 (duplicate)
            "gender"      // UPDATE position 11 (duplicate)
        )
        
        assertEquals(expectedParameterOrder, insertStatement.namedParameters,
            "Should capture all parameter occurrences including duplicates")
        
        conn.close()
    }

    @Test
    fun testInsertOnConflictWithMixedParameterNames() {
        // Test case with some same and some different parameter names
        val sql = """
            INSERT INTO UserProfile(id, name, email, status)
            VALUES (:id, :name, :email, :status)
            ON CONFLICT(id) DO UPDATE SET 
                name = :updatedName,
                email = :email,
                status = :newStatus,
                updated_at = :timestamp
        """.trimIndent()
        
        val statement = CCJSqlParserUtil.parse(sql) as Insert
        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        val insertStatement = InsertStatement.parse(statement, conn)
        
        val expectedParameterOrder = listOf(
            "id",           // VALUES position 1
            "name",         // VALUES position 2
            "email",        // VALUES position 3
            "status",       // VALUES position 4
            "updatedName",  // UPDATE position 5 (different name)
            "email",        // UPDATE position 6 (duplicate)
            "newStatus",    // UPDATE position 7 (different name)
            "timestamp"     // UPDATE position 8 (different name)
        )
        
        assertEquals(expectedParameterOrder, insertStatement.namedParameters)
        
        // Verify column mappings
        val columnMappings = insertStatement.columnNamesAssociatedWithNamedParameters
        assertEquals("id", columnMappings["id"])
        assertEquals("name", columnMappings["name"])
        assertEquals("email", columnMappings["email"])
        assertEquals("status", columnMappings["status"])
        assertEquals("name", columnMappings["updatedName"])
        assertEquals("status", columnMappings["newStatus"])
        assertEquals("updated_at", columnMappings["timestamp"])
        
        conn.close()
    }

    @Test
    fun testRegularInsertStillWorks() {
        // Ensure regular INSERT statements without ON CONFLICT still work
        val sql = """
            INSERT INTO Person(name, email, age)
            VALUES (:name, :email, :age)
        """.trimIndent()
        
        val statement = CCJSqlParserUtil.parse(sql) as Insert
        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        val insertStatement = InsertStatement.parse(statement, conn)
        
        assertEquals(listOf("name", "email", "age"), insertStatement.namedParameters)
        
        val columnMappings = insertStatement.columnNamesAssociatedWithNamedParameters
        assertEquals("name", columnMappings["name"])
        assertEquals("email", columnMappings["email"])
        assertEquals("age", columnMappings["age"])
        
        conn.close()
    }
}
