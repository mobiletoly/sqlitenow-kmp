package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.ParameterizedTypeName
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
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

    @TestFactory
    fun determinePropertyTypeCases(): List<DynamicTest> = listOf(
        PropertyTypeCase(
            displayName = "custom type annotation",
            propertyType = "java.time.LocalDateTime",
            isNullable = false,
            expectedType = "java.time.LocalDateTime",
            expectedNullable = false,
        ),
        PropertyTypeCase(
            displayName = "Kotlin standard library type annotation",
            propertyType = "String",
            isNullable = true,
            expectedType = "kotlin.String?",
            expectedNullable = true,
        ),
        PropertyTypeCase(
            displayName = "without type annotation",
            propertyType = null,
            isNullable = true,
            expectedType = "kotlin.Int?",
            expectedNullable = true,
        ),
    ).map { case ->
        DynamicTest.dynamicTest(case.displayName) {
            val baseType = ClassName("kotlin", "Int")

            val resultType = SqliteTypeToKotlinCodeConverter.determinePropertyType(
                baseType = baseType,
                propertyType = case.propertyType,
                isNullable = case.isNullable,
            )

            assertEquals(case.expectedType, resultType.toString())
            assertEquals(case.expectedNullable, resultType.isNullable)
        }
    }

    @TestFactory
    fun genericTypeParsingCases(): List<DynamicTest> = listOf(
        GenericTypeCase(
            displayName = "simple List",
            propertyType = "List<String>",
            expectedRawType = "kotlin.collections.List",
            expectedTypeArguments = listOf("kotlin.String"),
        ),
        GenericTypeCase(
            displayName = "custom generic type",
            propertyType = "com.example.CustomList<String>",
            expectedRawType = "com.example.CustomList",
            expectedTypeArguments = listOf("kotlin.String"),
        ),
    ).map { case ->
        DynamicTest.dynamicTest(case.displayName) {
            val baseType = ClassName("kotlin", "String")
            val resultType = SqliteTypeToKotlinCodeConverter.determinePropertyType(baseType, case.propertyType, false)

            assertTrue(resultType is ParameterizedTypeName, "Result should be a ParameterizedTypeName")
            val parameterizedType = resultType
            assertEquals(case.expectedRawType, parameterizedType.rawType.toString())
            assertEquals(case.expectedTypeArguments, parameterizedType.typeArguments.map { it.toString() })
        }
    }

    @Test
    @DisplayName("Test generic type parsing with nested generics")
    fun testGenericTypeParsingNestedGenerics() {
        val baseType = ClassName("kotlin", "String")
        val propertyType = "Map<String, List<Int>>"
        val isNullable = false

        val resultType = SqliteTypeToKotlinCodeConverter.determinePropertyType(baseType, propertyType, isNullable)
        assertTrue(resultType is ParameterizedTypeName, "Result should be a ParameterizedTypeName")
        val parameterizedType = resultType
        assertEquals("kotlin.collections.Map", parameterizedType.rawType.toString())
        assertEquals(2, parameterizedType.typeArguments.size)
        assertEquals("kotlin.String", parameterizedType.typeArguments[0].toString())

        val secondArg = parameterizedType.typeArguments[1]
        assertTrue(secondArg is ParameterizedTypeName, "Second argument should be parameterized")
        val nestedType = secondArg
        assertEquals("kotlin.collections.List", nestedType.rawType.toString())
        assertEquals("kotlin.Int", nestedType.typeArguments[0].toString())
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

    private data class GenericTypeCase(
        val displayName: String,
        val propertyType: String,
        val expectedRawType: String,
        val expectedTypeArguments: List<String>,
    )

    private data class PropertyTypeCase(
        val displayName: String,
        val propertyType: String?,
        val isNullable: Boolean,
        val expectedType: String,
        val expectedNullable: Boolean,
    )
}
