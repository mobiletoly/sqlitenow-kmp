package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.InsertStatement
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.insert.Insert
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import kotlin.test.assertEquals

class InsertOnConflictParameterOrderTest {

    private fun parseInsertStatement(sql: String): InsertStatement {
        val statement = CCJSqlParserUtil.parse(sql.trimIndent()) as Insert
        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        return try {
            InsertStatement.parse(statement, conn)
        } finally {
            conn.close()
        }
    }

    private fun assertColumnMappings(insertStatement: InsertStatement, expectedMappings: Map<String, String>) {
        val columnMappings = insertStatement.columnNamesAssociatedWithNamedParameters
        expectedMappings.forEach { (parameterName, columnName) ->
            assertEquals(columnName, columnMappings[parameterName])
        }
    }

    @TestFactory
    fun parameterOrderCases(): List<DynamicTest> {
        return listOf(
            InsertParameterOrderCase(
                displayName = "ON CONFLICT captures duplicate update parameters in source order",
                sql = """
                    INSERT INTO Person(email, first_name, last_name, phone, birth_date, notes)
                    VALUES (:email, :firstName, :lastName, :phone, :birthDate, :notes)
                    ON CONFLICT(email) DO UPDATE SET
                        first_name = :firstName,
                        last_name = :lastName,
                        phone = :phone,
                        birth_date = :birthDate,
                        notes = :notes2
                """,
                expectedParameterOrder = listOf(
                    "email",
                    "firstName",
                    "lastName",
                    "phone",
                    "birthDate",
                    "notes",
                    "firstName",
                    "lastName",
                    "phone",
                    "birthDate",
                    "notes2",
                ),
                expectedColumnMappings = mapOf(
                    "email" to "email",
                    "firstName" to "first_name",
                    "lastName" to "last_name",
                    "phone" to "phone",
                    "birthDate" to "birth_date",
                    "notes" to "notes",
                    "notes2" to "notes",
                )
            ),
            InsertParameterOrderCase(
                displayName = "ON CONFLICT captures repeated parameter names from VALUES and UPDATE",
                sql = """
                    INSERT INTO UserProfile(id, isMetric, nickname, height, birthday, gender)
                    VALUES ('main', :isMetric, :nickname, :height, :birthday, :gender)
                    ON CONFLICT(id) DO UPDATE SET
                        isMetric = :isMetric,
                        nickname = :nickname,
                        height = :height,
                        birthday = :birthday,
                        gender = :gender
                """,
                expectedParameterOrder = listOf(
                    "isMetric",
                    "nickname",
                    "height",
                    "birthday",
                    "gender",
                    "isMetric",
                    "nickname",
                    "height",
                    "birthday",
                    "gender",
                )
            ),
            InsertParameterOrderCase(
                displayName = "ON CONFLICT maps mixed update parameter names to target columns",
                sql = """
                    INSERT INTO UserProfile(id, name, email, status)
                    VALUES (:id, :name, :email, :status)
                    ON CONFLICT(id) DO UPDATE SET
                        name = :updatedName,
                        email = :email,
                        status = :newStatus,
                        updated_at = :timestamp
                """,
                expectedParameterOrder = listOf(
                    "id",
                    "name",
                    "email",
                    "status",
                    "updatedName",
                    "email",
                    "newStatus",
                    "timestamp",
                ),
                expectedColumnMappings = mapOf(
                    "id" to "id",
                    "name" to "name",
                    "email" to "email",
                    "status" to "status",
                    "updatedName" to "name",
                    "newStatus" to "status",
                    "timestamp" to "updated_at",
                )
            ),
            InsertParameterOrderCase(
                displayName = "regular INSERT still captures parameters and column mappings",
                sql = """
                    INSERT INTO Person(name, email, age)
                    VALUES (:name, :email, :age)
                """,
                expectedParameterOrder = listOf("name", "email", "age"),
                expectedColumnMappings = mapOf(
                    "name" to "name",
                    "email" to "email",
                    "age" to "age",
                )
            )
        ).map { case ->
            DynamicTest.dynamicTest(case.displayName) {
                val insertStatement = parseInsertStatement(case.sql)
                assertEquals(case.expectedParameterOrder, insertStatement.namedParameters)
                assertColumnMappings(insertStatement, case.expectedColumnMappings)
            }
        }
    }

    private data class InsertParameterOrderCase(
        val displayName: String,
        val sql: String,
        val expectedParameterOrder: List<String>,
        val expectedColumnMappings: Map<String, String> = emptyMap(),
    )
}
