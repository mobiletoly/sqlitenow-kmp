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

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.ParameterizedTypeName
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SqliteTypeToKotlinCodeConverterTest {

    @Test
    @DisplayName("Test mapping SQLite INTEGER type to Kotlin Long")
    fun testMapIntegerToLong() {
        val kotlinType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType("INTEGER")
        assertEquals("kotlin.Long", kotlinType.toString())
    }

    @Test
    @DisplayName("Test mapping SQLite TEXT type to Kotlin String")
    fun testMapTextToString() {
        val kotlinType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType("TEXT")
        assertEquals("kotlin.String", kotlinType.toString())
    }

    @Test
    @DisplayName("Test mapping SQLite REAL type to Kotlin Double")
    fun testMapRealToDouble() {
        val kotlinType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType("REAL")
        assertEquals("kotlin.Double", kotlinType.toString())
    }

    @Test
    @DisplayName("Test mapping SQLite BLOB type to Kotlin ByteArray")
    fun testMapBlobToByteArray() {
        val kotlinType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType("BLOB")
        assertEquals("kotlin.ByteArray", kotlinType.toString())
    }

    @Test
    @DisplayName("Test mapping unknown SQLite type to default type")
    fun testMapUnknownTypeToDefault() {
        val kotlinType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType("UNKNOWN_TYPE")
        assertEquals("kotlin.String", kotlinType.toString())
    }

    @Test
    @DisplayName("Test determining property type with custom type annotation")
    fun testDeterminePropertyTypeWithCustomType() {
        val baseType = ClassName("kotlin", "Int")
        val propertyType = "java.time.LocalDateTime"
        val isNullable = false

        val resultType = SqliteTypeToKotlinCodeConverter.determinePropertyType(baseType, propertyType, isNullable)
        assertEquals("java.time.LocalDateTime", resultType.toString())
        assertFalse(resultType.isNullable)
    }

    @Test
    @DisplayName("Test determining property type with Kotlin standard library type annotation")
    fun testDeterminePropertyTypeWithKotlinStdlibType() {
        val baseType = ClassName("kotlin", "Int")
        val propertyType = "String"
        val isNullable = true

        val resultType = SqliteTypeToKotlinCodeConverter.determinePropertyType(baseType, propertyType, isNullable)
        assertEquals("kotlin.String?", resultType.toString())
        assertTrue(resultType.isNullable)
    }

    @Test
    @DisplayName("Test determining property type without type annotation")
    fun testDeterminePropertyTypeWithoutAnnotation() {
        val baseType = ClassName("kotlin", "Int")
        val propertyType: String? = null
        val isNullable = true

        val resultType = SqliteTypeToKotlinCodeConverter.determinePropertyType(baseType, propertyType, isNullable)
        assertEquals("kotlin.Int?", resultType.toString())
        assertTrue(resultType.isNullable)
    }

    @Test
    @DisplayName("Test generic type parsing with simple List")
    fun testGenericTypeParsingSimpleList() {
        val baseType = ClassName("kotlin", "String")
        val propertyType = "List<String>"
        val isNullable = false

        val resultType = SqliteTypeToKotlinCodeConverter.determinePropertyType(baseType, propertyType, isNullable)
        assertTrue(resultType is ParameterizedTypeName, "Result should be a ParameterizedTypeName")
        val parameterizedType = resultType as ParameterizedTypeName
        assertEquals("kotlin.collections.List", parameterizedType.rawType.toString())
        assertEquals(1, parameterizedType.typeArguments.size)
        assertEquals("kotlin.String", parameterizedType.typeArguments[0].toString())
    }

    @Test
    @DisplayName("Test generic type parsing with nested generics")
    fun testGenericTypeParsingNestedGenerics() {
        val baseType = ClassName("kotlin", "String")
        val propertyType = "Map<String, List<Int>>"
        val isNullable = false

        val resultType = SqliteTypeToKotlinCodeConverter.determinePropertyType(baseType, propertyType, isNullable)
        assertTrue(resultType is ParameterizedTypeName, "Result should be a ParameterizedTypeName")
        val parameterizedType = resultType as ParameterizedTypeName
        assertEquals("kotlin.collections.Map", parameterizedType.rawType.toString())
        assertEquals(2, parameterizedType.typeArguments.size)
        assertEquals("kotlin.String", parameterizedType.typeArguments[0].toString())

        val secondArg = parameterizedType.typeArguments[1]
        assertTrue(secondArg is ParameterizedTypeName, "Second argument should be parameterized")
        val nestedType = secondArg as ParameterizedTypeName
        assertEquals("kotlin.collections.List", nestedType.rawType.toString())
        assertEquals("kotlin.Int", nestedType.typeArguments[0].toString())
    }

    @Test
    @DisplayName("Test generic type parsing with custom types")
    fun testGenericTypeParsingCustomTypes() {
        val baseType = ClassName("kotlin", "String")
        val propertyType = "com.example.CustomList<String>"
        val isNullable = false

        val resultType = SqliteTypeToKotlinCodeConverter.determinePropertyType(baseType, propertyType, isNullable)
        assertTrue(resultType is ParameterizedTypeName, "Result should be a ParameterizedTypeName")
        val parameterizedType = resultType as ParameterizedTypeName
        assertEquals("com.example.CustomList", parameterizedType.rawType.toString())
        assertEquals(1, parameterizedType.typeArguments.size)
        assertEquals("kotlin.String", parameterizedType.typeArguments[0].toString())
    }

    @Test
    @DisplayName("Test nullable generic type")
    fun testNullableGenericType() {
        val baseType = ClassName("kotlin", "String")
        val propertyType = "List<String>"
        val isNullable = true

        val resultType = SqliteTypeToKotlinCodeConverter.determinePropertyType(baseType, propertyType, isNullable)
        assertTrue(resultType.isNullable, "Result should be nullable")
        assertTrue(resultType is ParameterizedTypeName, "Result should be a ParameterizedTypeName")
    }
}
