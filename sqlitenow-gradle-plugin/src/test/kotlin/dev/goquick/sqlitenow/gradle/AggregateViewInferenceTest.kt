/*
 * Copyright 2025 Anatoliy Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationResolver
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import dev.goquick.sqlitenow.gradle.processing.StatementProcessingHelper
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateViewStatement
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.create.view.CreateView
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class AggregateViewInferenceTest {

    private lateinit var conn: Connection

    @BeforeEach
    fun setUp() {
        conn = DriverManager.getConnection("jdbc:sqlite::memory:")
        conn.createStatement().use { stmt ->
            stmt.execute(
                """
                CREATE TABLE item (
                    id INTEGER PRIMARY KEY,
                    item_type TEXT NOT NULL,
                    name TEXT NOT NULL
                );
                """.trimIndent()
            )
        }
        conn.createStatement().use { stmt ->
            stmt.execute(
                """
                CREATE VIEW item_metrics_view AS
                SELECT
                    item_type,
                    COUNT(*) AS total_count,
                    GROUP_CONCAT(name, ',') AS grouped_names
                FROM item
                GROUP BY item_type;
                """.trimIndent()
            )
        }
    }

    @AfterEach
    fun tearDown() {
        conn.close()
    }

    @Test
    fun selectStarFromAggregatedViewCarriesExpressions() {
        val viewSql = """
            CREATE VIEW item_metrics_view AS
            SELECT
                item_type,
                COUNT(*) AS total_count,
                GROUP_CONCAT(name, ',') AS grouped_names
            FROM item
            GROUP BY item_type;
        """.trimIndent()

        val createView = CCJSqlParserUtil.parse(viewSql) as CreateView
        val viewStatement = CreateViewStatement.parse(viewSql, createView, conn)
        val annotatedView = AnnotatedCreateViewStatement.parse(
            name = "ItemMetricsView",
            createViewStatement = viewStatement,
            topComments = emptyList(),
            innerComments = emptyList(),
        )

        val helper = StatementProcessingHelper(
            conn = conn,
            annotationResolver = FieldAnnotationResolver(
                createTableStatements = emptyList(),
                createViewStatements = listOf(annotatedView)
            )
        )

        val querySql = "SELECT * FROM item_metrics_view;"
        val queryFile = Files.createTempFile("item_metrics_query", ".sql").toFile()
        queryFile.writeText(querySql)

        val annotated = helper.processQueryFile(queryFile) as AnnotatedSelectStatement
        val generator = SelectFieldCodeGenerator(
            createTableStatements = emptyList(),
            createViewStatements = listOf(annotatedView),
            packageName = "dev.goquick.sqlitenow.test"
        )

        val countField = annotated.fields.first { it.src.fieldName.equals("total_count", ignoreCase = true) }
        assertNotNull(countField.src.expression, "COUNT expression should be preserved")
        assertIs<net.sf.jsqlparser.expression.Function>(countField.src.expression)
        val countProperty = generator.generateProperty(countField)
        assertEquals("totalCount", countProperty.name)
        assertEquals("kotlin.Long", countProperty.type.toString())

        val namesField = annotated.fields.first { it.src.fieldName.equals("grouped_names", ignoreCase = true) }
        assertNotNull(namesField.src.expression, "GROUP_CONCAT expression should be preserved")
        val namesProperty = generator.generateProperty(namesField)
        assertEquals("groupedNames", namesProperty.name)
        assertEquals("kotlin.String?", namesProperty.type.toString())

        val tableStatement = CreateTableStatement(
            sql = "",
            tableName = "item_metrics_view",
            columns = emptyList()
        )
        val annotatedTable = AnnotatedCreateTableStatement(
            name = "ItemMetricsView",
            src = tableStatement,
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            columns = emptyList()
        )
        assertTrue(annotatedTable.columns.isEmpty())
    }

    @Test
    fun sqlTypeHintOverridesMetadataType() {
        val viewSql = """
            CREATE VIEW item_metrics_view AS
            SELECT
                item_type,
                COUNT(*) AS total_count,
                GROUP_CONCAT(name, ',') AS grouped_names
            FROM item
            GROUP BY item_type;
        """.trimIndent()

        val createView = CCJSqlParserUtil.parse(viewSql) as CreateView
        val viewStatement = CreateViewStatement.parse(viewSql, createView, conn)
        val annotatedView = AnnotatedCreateViewStatement.parse(
            name = "ItemMetricsView",
            createViewStatement = viewStatement,
            topComments = emptyList(),
            innerComments = listOf("-- @@{ field=grouped_names, sqlTypeHint=TEXT, notNull=true }"),
        )

        val helper = StatementProcessingHelper(
            conn = conn,
            annotationResolver = FieldAnnotationResolver(
                createTableStatements = emptyList(),
                createViewStatements = listOf(annotatedView)
            )
        )

        val querySql = "SELECT * FROM item_metrics_view;"
        val queryFile = Files.createTempFile("item_metrics_query_hint", ".sql").toFile()
        queryFile.writeText(querySql)

        val annotated = helper.processQueryFile(queryFile) as AnnotatedSelectStatement
        val groupedField = annotated.fields.first { it.src.fieldName.equals("grouped_names", ignoreCase = true) }
        assertEquals("TEXT", groupedField.src.dataType)
        assertEquals(false, groupedField.src.isNullable)
    }
}
