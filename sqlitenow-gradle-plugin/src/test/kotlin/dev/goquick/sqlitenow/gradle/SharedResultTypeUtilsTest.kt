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
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SharedResultTypeUtils
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName

class SharedResultTypeUtilsTest {

    @Test
    @DisplayName("Test SharedResultTypeUtils creates correct shared result type names")
    fun testCreateSharedResultTypeName() {
        val typeName = SharedResultTypeUtils.createSharedResultTypeName(
            packageName = "com.example.db",
            namespace = "person",
            sharedResultName = "All"
        )

        assertEquals("com.example.db.PersonQuery.SharedResult.All", typeName.toString())
    }

    @Test
    @DisplayName("Test SharedResultTypeUtils creates correct result type names for shared results")
    fun testCreateResultTypeNameWithSharedResult() {
        val statement = AnnotatedSelectStatement(
            name = "SelectAll",
            src = SelectStatement(
                sql = "SELECT * FROM Person",
                fromTable = "person",
                joinTables = emptyList(),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = emptyList()
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "All",
                collectionKey = null
            ),
            fields = emptyList()
        )

        val typeName = SharedResultTypeUtils.createResultTypeName(
            packageName = "com.example.db",
            namespace = "person",
            statement = statement
        )

        assertEquals("com.example.db.All", typeName.toString())
    }

    @Test
    @DisplayName("Test SharedResultTypeUtils creates correct result type names for regular results")
    fun testCreateResultTypeNameWithRegularResult() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWeird",
            src = SelectStatement(
                sql = "SELECT * FROM Person WHERE weird = 1",
                fromTable = "person",
                joinTables = emptyList(),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = emptyList()
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            fields = emptyList()
        )

        val typeName = SharedResultTypeUtils.createResultTypeName(
            packageName = "com.example.db",
            namespace = "person",
            statement = statement
        )

        assertEquals("com.example.db.PersonSelectWeirdResult", typeName.toString())
    }

    @Test
    @DisplayName("Test SharedResultTypeUtils creates correct result type strings")
    fun testCreateResultTypeString() {
        val sharedStatement = AnnotatedSelectStatement(
            name = "SelectAll",
            src = SelectStatement(
                sql = "SELECT * FROM Person",
                fromTable = "person",
                joinTables = emptyList(),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = emptyList()
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "All",
                collectionKey = null
            ),
            fields = emptyList()
        )

        val sharedResultString = SharedResultTypeUtils.createResultTypeString("person", sharedStatement)
        assertEquals("All", sharedResultString)

        val regularStatement = AnnotatedSelectStatement(
            name = "SelectWeird",
            src = SelectStatement(
                sql = "SELECT * FROM Person WHERE weird = 1",
                fromTable = "person",
                joinTables = emptyList(),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = emptyList()
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            fields = emptyList()
        )

        val regularResultString = SharedResultTypeUtils.createResultTypeString("person", regularStatement)
        assertEquals("PersonSelectWeirdResult", regularResultString)
    }

    @Test
    @DisplayName("Test that SHARED_RESULT_OBJECT_NAME constant is used consistently")
    fun testSharedResultObjectNameConstant() {
        assertEquals("SharedResult", SharedResultTypeUtils.SHARED_RESULT_OBJECT_NAME)

        // Verify that changing the constant would affect all generated type names
        val typeName = SharedResultTypeUtils.createSharedResultTypeName(
            packageName = "com.example.db",
            namespace = "person",
            sharedResultName = "All"
        )

        assertTrue(typeName.toString().contains(SharedResultTypeUtils.SHARED_RESULT_OBJECT_NAME))
    }
}
