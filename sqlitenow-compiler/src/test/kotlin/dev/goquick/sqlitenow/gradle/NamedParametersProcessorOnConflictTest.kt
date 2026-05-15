package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.NamedParametersProcessor
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.insert.Insert
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class NamedParametersProcessorOnConflictTest {

    @Test
    fun testNamedParametersProcessorReplacesOnConflictParameters() {
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

        val expectedParameters = listOf(
            "email", "firstName", "lastName", "phone", "birthDate", "notes",
            "firstName", "lastName", "phone", "birthDate", "notes2",
        )
        val processor = assertProcessedInsert(
            sql = sql,
            expectedParameters = expectedParameters,
            expectedSqlFragments = listOf(
                "INSERT INTO Person",
                "VALUES (?, ?, ?, ?, ?, ?)",
                "ON CONFLICT",
                "DO UPDATE SET",
            ),
        )

        val processedSqlNormalized = processor.processedSql.replace("\\s+".toRegex(), " ").trim()
        val updateSetPart = processedSqlNormalized.substringAfter("DO UPDATE SET")
        assertFalse(updateSetPart.contains(":"), "UPDATE SET clause should not contain any : parameters")
        assertEquals(5, updateSetPart.count { it == '?' }, "UPDATE clause should have 5 ? placeholders")
    }

    @Test
    fun testNamedParametersProcessorWithSimpleOnConflict() {
        val sql = """
            INSERT INTO Person(name, email) 
            VALUES (:name, :email) 
            ON CONFLICT(email) DO UPDATE SET name = :updatedName
        """.trimIndent()

        assertProcessedInsert(
            sql = sql,
            expectedParameters = listOf("name", "email", "updatedName"),
        )
    }

    @Test
    fun testNamedParametersProcessorWithDuplicateParameters() {
        val sql = """
            INSERT INTO Person(name, email) 
            VALUES (:name, :email) 
            ON CONFLICT(email) DO UPDATE SET name = :name, email = :email
        """.trimIndent()

        assertProcessedInsert(
            sql = sql,
            expectedParameters = listOf("name", "email", "name", "email"),
        )
    }

    @Test
    fun testNamedParametersProcessorWithMixedLiteralsAndParameters() {
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

        assertProcessedInsert(
            sql = sql,
            expectedParameters = listOf(
                "firstName", "lastName", "phone", "birthDate", "notes",
                "firstName", "lastName", "birthDate2", "notes",
            ),
            expectedSqlFragments = listOf(
                "'hello@world.com'",
                "'333-444-5555'",
                "VALUES ('hello@world.com', ?, ?, ?, ?, ?)",
                "phone = '333-444-5555'",
            ),
        )
    }

    @TestFactory
    fun namedParametersProcessorPreservesConflictSqlStructure(): List<DynamicTest> = listOf(
        ConflictSqlCase(
            displayName = "conflict target and where predicate",
            sql = """
                INSERT INTO Person(name, email, active)
                VALUES (:name, :email, :active)
                ON CONFLICT(email) WHERE active = 1
                DO UPDATE SET name = :updatedName
                WHERE Person.active = :onlyWhenActive
            """,
            expectedSqlFragments = listOf(
                "ON CONFLICT (email) WHERE active = 1",
                "DO UPDATE SET name = ? WHERE Person.active = ?",
            ),
            expectedParameters = listOf("name", "email", "active", "updatedName", "onlyWhenActive"),
        ),
        ConflictSqlCase(
            displayName = "do nothing conflict action",
            sql = """
                INSERT INTO Person(name, email)
                VALUES (:name, :email)
                ON CONFLICT(email) DO NOTHING
            """,
            expectedSqlFragments = listOf("ON CONFLICT (email) DO NOTHING"),
            expectedParameters = listOf("name", "email"),
        ),
        ConflictSqlCase(
            displayName = "insert select structure",
            sql = """
                INSERT INTO PersonArchive(id, email)
                SELECT p.id, :overrideEmail
                FROM Person p
                WHERE p.group_id = :groupId
                ON CONFLICT(id) DO UPDATE SET email = :updatedEmail
            """,
            expectedSqlFragments = listOf(
                "INSERT INTO PersonArchive (id, email) SELECT p.id, ? FROM Person p WHERE p.group_id = ?",
                "ON CONFLICT (id) DO UPDATE SET email = ?",
            ),
            expectedParameters = listOf("overrideEmail", "groupId", "updatedEmail"),
        ),
    ).map { case ->
        DynamicTest.dynamicTest(case.displayName) {
            val processor = processInsert(case.sql)
            val normalized = processor.processedSql.normalizedSql()

            case.expectedSqlFragments.forEach { fragment ->
                assertTrue(normalized.contains(fragment))
            }
            assertEquals(case.expectedParameters, processor.parameters)
            assertEquals(case.expectedParameters.size, processor.processedSql.count { it == '?' })
        }
    }

    @Test
    fun testNamedParametersProcessorKeepsWithClauseParameterOrderForInsert() {
        val sql = """
            WITH selected AS (
                SELECT id
                FROM Person
                WHERE group_id = :groupId
            )
            INSERT INTO PersonArchive(id, nickname)
            SELECT id, :nickname
            FROM selected
            ON CONFLICT(id) DO UPDATE SET nickname = :updatedNickname
        """.trimIndent()

        val statement = CCJSqlParserUtil.parse(sql) as Insert
        val processor = NamedParametersProcessor(statement)

        assertEquals(listOf("groupId", "nickname", "updatedNickname"), processor.parameters)
        assertTrue(processor.processedSql.contains("WITH selected AS"))
    }

    private fun processInsert(sql: String): NamedParametersProcessor {
        val statement = CCJSqlParserUtil.parse(sql.trimIndent()) as Insert
        return NamedParametersProcessor(statement)
    }

    private fun assertProcessedInsert(
        sql: String,
        expectedParameters: List<String>,
        expectedSqlFragments: List<String> = emptyList(),
    ): NamedParametersProcessor {
        val processor = processInsert(sql)
        val normalized = processor.processedSql.normalizedSql()

        expectedParameters.distinct().forEach { parameter ->
            assertFalse(processor.processedSql.contains(":$parameter"), "Processed SQL should not contain :$parameter")
        }
        assertEquals(expectedParameters, processor.parameters, "Parameters should be in correct order")
        assertEquals(
            expectedParameters.size,
            processor.processedSql.count { it == '?' },
            "Number of ? placeholders should match number of parameters"
        )
        expectedSqlFragments.forEach { fragment ->
            assertTrue(normalized.contains(fragment), "Processed SQL should contain $fragment")
        }
        return processor
    }

    private fun String.normalizedSql(): String = replace("\\s+".toRegex(), " ")
        .trim()
        .replace("( ", "(")
        .replace(" )", ")")

    private data class ConflictSqlCase(
        val displayName: String,
        val sql: String,
        val expectedSqlFragments: List<String>,
        val expectedParameters: List<String>,
    )
}
