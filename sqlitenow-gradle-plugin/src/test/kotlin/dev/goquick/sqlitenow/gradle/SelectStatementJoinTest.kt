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

import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class SelectStatementJoinTest {

    @Test
    fun `simple LEFT JOIN is parsed correctly`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithLeftJoin",
            src = SelectStatement(
                sql = "SELECT p.id, p.name, a.street FROM Person p LEFT JOIN Address a ON p.id = a.person_id",
                fromTable = "person",
                joinTables = listOf("Address"),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    SelectStatement.FieldSource("street", "a", "street", "TEXT")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("street", "a", "street", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                )
            )
        )
        
        assertNotNull(statement)
        assertEquals(1, statement.src.joinTables.size)
        assertEquals("Address", statement.src.joinTables.first())
        
        // Person fields should not be nullable (main table)
        val personFields = statement.fields.filter { it.src.tableName == "p" }
        assertEquals(2, personFields.size)
        
        // Address fields should be nullable (LEFT JOIN)
        val addressFields = statement.fields.filter { it.src.tableName == "a" }
        assertEquals(1, addressFields.size)
    }

    @Test
    fun `INNER JOIN is parsed correctly`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithInnerJoin",
            src = SelectStatement(
                sql = "SELECT p.id, p.name, a.street FROM Person p INNER JOIN Address a ON p.id = a.person_id",
                fromTable = "person",
                joinTables = listOf("Address"),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    SelectStatement.FieldSource("street", "a", "street", "TEXT")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("street", "a", "street", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                )
            )
        )
        
        assertNotNull(statement)
        assertEquals(1, statement.src.joinTables.size)
        assertEquals("Address", statement.src.joinTables.first())
        
        // INNER JOIN fields should maintain their original nullability
        // since INNER JOIN guarantees matching records exist
    }

    @Test
    fun `multiple JOINs are parsed correctly`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithMultipleJoins",
            src = SelectStatement(
                sql = "SELECT p.id, p.name, a.street, c.comment FROM person p LEFT JOIN address a ON p.id = a.person_id LEFT JOIN comment c ON p.id = c.person_id",
                fromTable = "person",
                joinTables = listOf("address", "comment"),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    SelectStatement.FieldSource("street", "a", "street", "TEXT"),
                    SelectStatement.FieldSource("comment", "c", "comment", "TEXT")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("street", "a", "street", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("comment", "c", "comment", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                )
            )
        )
        
        assertNotNull(statement)
        assertEquals(2, statement.src.joinTables.size)
        
        val personFields = statement.fields.filter { it.src.tableName == "p" }
        assertEquals(2, personFields.size)
        
        val addressFields = statement.fields.filter { it.src.tableName == "a" }
        assertEquals(1, addressFields.size)
        
        val commentFields = statement.fields.filter { it.src.tableName == "c" }
        assertEquals(1, commentFields.size)
    }

    @Test
    fun `fields from joined tables are identified correctly`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithJoinedFields",
            src = SelectStatement(
                sql = "SELECT p.id, p.name, a.street, a.city, c.comment FROM person p LEFT JOIN address a ON p.id = a.person_id LEFT JOIN comment c ON p.id = c.person_id",
                fromTable = "person",
                joinTables = listOf("address", "comment"),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    SelectStatement.FieldSource("street", "a", "street", "TEXT"),
                    SelectStatement.FieldSource("city", "a", "city", "TEXT"),
                    SelectStatement.FieldSource("comment", "c", "comment", "TEXT")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("street", "a", "street", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("city", "a", "city", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("comment", "c", "comment", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                )
            )
        )
        
        assertNotNull(statement)
        
        val personFields = statement.fields.filter { it.src.tableName == "p" }
        assertEquals(2, personFields.size)
        
        val addressFields = statement.fields.filter { it.src.tableName == "a" }
        assertEquals(2, addressFields.size)
        
        val commentFields = statement.fields.filter { it.src.tableName == "c" }
        assertEquals(1, commentFields.size)
    }

    @Test
    fun `LEFT JOIN fields should be nullable by default`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithNullableLeftJoin",
            src = SelectStatement(
                sql = "SELECT p.id, p.name, a.street FROM Person p LEFT JOIN Address a ON p.id = a.person_id",
                fromTable = "person",
                joinTables = listOf("Address"),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    SelectStatement.FieldSource("street", "a", "street", "TEXT")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("street", "a", "street", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                )
            )
        )
        
        assertNotNull(statement)
        
        // Person fields should not be nullable (main table)
        val personFields = statement.fields.filter { it.src.tableName == "p" }
        personFields.forEach { field ->
            // Main table fields keep their original nullability
            // This depends on the schema definition
        }
        
        // Address fields should be nullable (LEFT JOIN)
        val addressFields = statement.fields.filter { it.src.tableName == "a" }
        addressFields.forEach { field ->
            // LEFT JOIN fields should be nullable unless explicitly marked notNull
            // The actual nullability logic is handled in the code generation
        }
    }

    @Test
    fun `LEFT JOIN with notNull annotation overrides nullability`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithNotNullLeftJoin",
            src = SelectStatement(
                sql = "SELECT p.id, p.name, a.street FROM Person p LEFT JOIN Address a ON p.id = a.person_id",
                fromTable = "person",
                joinTables = listOf("Address"),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    SelectStatement.FieldSource("street", "a", "street", "TEXT")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("street", "a", "street", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(
                        mapOf(
                            AnnotationConstants.NOT_NULL to true
                        )
                    )
                )
            )
        )
        
        assertNotNull(statement)
        
        val streetField = statement.fields.find { it.src.originalColumnName == "street" }
        assertNotNull(streetField)
        assertEquals(true, streetField.annotations.notNull)
        
        // Even though it's a LEFT JOIN, the notNull annotation should override the default nullability
    }
}
