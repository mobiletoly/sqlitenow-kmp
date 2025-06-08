package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
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
}
