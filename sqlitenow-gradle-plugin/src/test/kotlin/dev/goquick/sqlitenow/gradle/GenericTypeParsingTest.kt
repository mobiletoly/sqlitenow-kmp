package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.ParameterizedTypeName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class GenericTypeParsingTest {

    @Test
    fun testSimpleGenericType() {
        val result = SqliteTypeToKotlinCodeConverter.determinePropertyType(
            baseType = ClassName("kotlin", "String"),
            propertyType = "List<String>",
            isNullable = false
        )
        
        assertTrue(result is ParameterizedTypeName, "Result should be a ParameterizedTypeName")
        val parameterizedType = result as ParameterizedTypeName
        assertEquals("kotlin.collections.List", parameterizedType.rawType.toString())
        assertEquals(1, parameterizedType.typeArguments.size)
        assertEquals("kotlin.String", parameterizedType.typeArguments[0].toString())
    }

    @Test
    fun testNestedGenericType() {
        val result = SqliteTypeToKotlinCodeConverter.determinePropertyType(
            baseType = ClassName("kotlin", "String"),
            propertyType = "Map<String, List<Int>>",
            isNullable = false
        )
        
        assertTrue(result is ParameterizedTypeName, "Result should be a ParameterizedTypeName")
        val parameterizedType = result as ParameterizedTypeName
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
    fun testCustomGenericType() {
        val result = SqliteTypeToKotlinCodeConverter.determinePropertyType(
            baseType = ClassName("kotlin", "String"),
            propertyType = "com.example.CustomList<String>",
            isNullable = false
        )
        
        assertTrue(result is ParameterizedTypeName, "Result should be a ParameterizedTypeName")
        val parameterizedType = result as ParameterizedTypeName
        assertEquals("com.example.CustomList", parameterizedType.rawType.toString())
        assertEquals(1, parameterizedType.typeArguments.size)
        assertEquals("kotlin.String", parameterizedType.typeArguments[0].toString())
    }

    @Test
    fun testNullableGenericType() {
        val result = SqliteTypeToKotlinCodeConverter.determinePropertyType(
            baseType = ClassName("kotlin", "String"),
            propertyType = "List<String>",
            isNullable = true
        )
        
        assertTrue(result.isNullable, "Result should be nullable")
        assertTrue(result is ParameterizedTypeName, "Result should be a ParameterizedTypeName")
    }

    @Test
    fun testSimpleTypeStillWorks() {
        val result = SqliteTypeToKotlinCodeConverter.determinePropertyType(
            baseType = ClassName("kotlin", "String"),
            propertyType = "kotlinx.datetime.LocalDate",
            isNullable = false
        )
        
        assertEquals("kotlinx.datetime.LocalDate", result.toString())
    }

    @Test
    fun testKotlinStdlibTypeStillWorks() {
        val result = SqliteTypeToKotlinCodeConverter.determinePropertyType(
            baseType = ClassName("kotlin", "Long"),
            propertyType = "String",
            isNullable = false
        )
        
        assertEquals("kotlin.String", result.toString())
    }

    @Test
    fun testMultipleTypeArguments() {
        val result = SqliteTypeToKotlinCodeConverter.determinePropertyType(
            baseType = ClassName("kotlin", "String"),
            propertyType = "Map<String, Int>",
            isNullable = false
        )
        
        assertTrue(result is ParameterizedTypeName, "Result should be a ParameterizedTypeName")
        val parameterizedType = result as ParameterizedTypeName
        assertEquals("kotlin.collections.Map", parameterizedType.rawType.toString())
        assertEquals(2, parameterizedType.typeArguments.size)
        assertEquals("kotlin.String", parameterizedType.typeArguments[0].toString())
        assertEquals("kotlin.Int", parameterizedType.typeArguments[1].toString())
    }
}
