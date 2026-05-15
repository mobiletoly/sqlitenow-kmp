package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.generator.data.DataStructCodeGenerator
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.processing.AffectedTablesResolver
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.sqlinspect.AssociatedColumn
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertTrue
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.io.TempDir

class CascadeNotifyGenerationTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var connection: Connection
    private lateinit var generator: DataStructCodeGenerator

    @BeforeEach
    fun setUp() {
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")

        val parentTable = AnnotatedCreateTableStatement(
            name = "Parent",
            src = CreateTableStatement(
                sql = "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                tableName = "parent",
                columns = listOf(
                    CreateTableStatement.Column(
                        name = "id",
                        dataType = "INTEGER",
                        notNull = true,
                        primaryKey = true,
                        autoIncrement = false,
                        unique = false,
                    ),
                    CreateTableStatement.Column(
                        name = "name",
                        dataType = "TEXT",
                        notNull = true,
                        primaryKey = false,
                        autoIncrement = false,
                        unique = false,
                    ),
                ),
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null,
                enableSync = false,
                syncKeyColumnName = null,
                mapTo = null,
                cascadeNotify = StatementAnnotationOverrides.CascadeNotify(
                    insert = emptySet(),
                    update = setOf("child"),
                    delete = setOf("child"),
                ),
            ),
            columns = listOf(
                AnnotatedCreateTableStatement.Column(
                    src = CreateTableStatement.Column(
                        name = "id",
                        dataType = "INTEGER",
                        notNull = true,
                        primaryKey = true,
                        autoIncrement = false,
                        unique = false,
                    ),
                    annotations = emptyMap(),
                ),
                AnnotatedCreateTableStatement.Column(
                    src = CreateTableStatement.Column(
                        name = "name",
                        dataType = "TEXT",
                        notNull = true,
                        primaryKey = false,
                        autoIncrement = false,
                        unique = false,
                    ),
                    annotations = emptyMap(),
                ),
            ),
        )

        val childTable = AnnotatedCreateTableStatement(
            name = "Child",
            src = CreateTableStatement(
                sql = "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL)",
                tableName = "child",
                columns = listOf(
                    CreateTableStatement.Column(
                        name = "id",
                        dataType = "INTEGER",
                        notNull = true,
                        primaryKey = true,
                        autoIncrement = false,
                        unique = false,
                    ),
                    CreateTableStatement.Column(
                        name = "parent_id",
                        dataType = "INTEGER",
                        notNull = true,
                        primaryKey = false,
                        autoIncrement = false,
                        unique = false,
                    ),
                ),
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null,
                enableSync = false,
                syncKeyColumnName = null,
                mapTo = null,
                cascadeNotify = StatementAnnotationOverrides.CascadeNotify(
                    insert = emptySet(),
                    update = setOf("grandchild"),
                    delete = setOf("grandchild"),
                ),
            ),
            columns = listOf(
                AnnotatedCreateTableStatement.Column(
                    src = CreateTableStatement.Column(
                        name = "id",
                        dataType = "INTEGER",
                        notNull = true,
                        primaryKey = true,
                        autoIncrement = false,
                        unique = false,
                    ),
                    annotations = emptyMap(),
                ),
                AnnotatedCreateTableStatement.Column(
                    src = CreateTableStatement.Column(
                        name = "parent_id",
                        dataType = "INTEGER",
                        notNull = true,
                        primaryKey = false,
                        autoIncrement = false,
                        unique = false,
                    ),
                    annotations = emptyMap(),
                ),
            ),
        )

        val grandchildTable = AnnotatedCreateTableStatement(
            name = "Grandchild",
            src = CreateTableStatement(
                sql = "CREATE TABLE grandchild(id INTEGER PRIMARY KEY, child_id INTEGER NOT NULL)",
                tableName = "grandchild",
                columns = listOf(
                    CreateTableStatement.Column(
                        name = "id",
                        dataType = "INTEGER",
                        notNull = true,
                        primaryKey = true,
                        autoIncrement = false,
                        unique = false,
                    ),
                    CreateTableStatement.Column(
                        name = "child_id",
                        dataType = "INTEGER",
                        notNull = true,
                        primaryKey = false,
                        autoIncrement = false,
                        unique = false,
                    ),
                ),
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null,
            ),
            columns = listOf(
                AnnotatedCreateTableStatement.Column(
                    src = CreateTableStatement.Column(
                        name = "id",
                        dataType = "INTEGER",
                        notNull = true,
                        primaryKey = true,
                        autoIncrement = false,
                        unique = false,
                    ),
                    annotations = emptyMap(),
                ),
                AnnotatedCreateTableStatement.Column(
                    src = CreateTableStatement.Column(
                        name = "child_id",
                        dataType = "INTEGER",
                        notNull = true,
                        primaryKey = false,
                        autoIncrement = false,
                        unique = false,
                    ),
                    annotations = emptyMap(),
                ),
            ),
        )

        val queriesDir = tempDir.resolve("queries").toFile().also { it.mkdirs() }

        generator = createDataStructCodeGeneratorWithMockExecutors(
            conn = connection,
            queriesDir = queriesDir,
            createTableStatements = listOf(parentTable, childTable, grandchildTable),
            packageName = "com.example.db",
            outputDir = tempDir.resolve("output").toFile(),
        )
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @TestFactory
    fun `statements inherit cascade notify tables`(): List<DynamicTest> = listOf(
        DynamicTest.dynamicTest("delete statement inherits cascade notify tables") {
            assertAffectedTablesIncludeCascadeTargets(deleteParentStatement())
        },
        DynamicTest.dynamicTest("update statement inherits cascade notify tables") {
            assertAffectedTablesIncludeCascadeTargets(updateParentStatement())
        },
    )

    private fun deleteParentStatement(): AnnotatedExecuteStatement =
        AnnotatedExecuteStatement(
            name = "deleteParent",
            src = DeleteStatement(
                sql = "DELETE FROM parent WHERE id = :id",
                table = "parent",
                namedParameters = listOf("id"),
                namedParametersToColumns = mapOf("id" to AssociatedColumn.Default("id")),
                withSelectStatements = emptyList(),
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null,
            ),
        )

    private fun updateParentStatement(): AnnotatedExecuteStatement =
        AnnotatedExecuteStatement(
            name = "updateParent",
            src = UpdateStatement(
                sql = "UPDATE parent SET name = :name WHERE id = :id",
                table = "parent",
                namedParameters = listOf("name", "id"),
                namedParametersToColumns = mapOf("id" to AssociatedColumn.Default("id")),
                namedParametersToColumnNames = mapOf("name" to "name"),
                withSelectStatements = emptyList(),
                parameterCastTypes = emptyMap(),
                hasReturningClause = false,
                returningColumns = emptyList(),
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null,
            ),
        )

    private fun assertAffectedTablesIncludeCascadeTargets(statement: AnnotatedExecuteStatement) {
        val resolver = AffectedTablesResolver.fromStatements(
            createTableStatements = generator.createTableStatements,
            createViewStatements = generator.createViewStatements,
            includeSchemaStatements = true,
        )
        val affectedTables = resolver.tablesFor(statement)
        assertTrue("parent" in affectedTables, "Base table should be included in affected tables")
        assertTrue("child" in affectedTables, "Direct cascade targets should be merged into affected tables")
        assertTrue("grandchild" in affectedTables, "Transitive cascade targets should be merged into affected tables")
    }
}
