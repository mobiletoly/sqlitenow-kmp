package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
import dev.goquick.sqlitenow.gradle.context.TypeMapping
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class TypeMappingServiceTest {

    private val typeMapping = TypeMapping()

    @Test
    @DisplayName("Test binding calls for standard Kotlin types")
    fun testBindingCallsForStandardTypes() {
        // Test Int binding
        val intType = ClassName("kotlin", "Int")
        assertEquals(
            "statement.bindInt(1, myValue)",
            typeMapping.getBindingCall(intType, 0, "myValue")
        )

        // Test Long binding
        val longType = ClassName("kotlin", "Long")
        assertEquals(
            "statement.bindLong(2, params.id)",
            typeMapping.getBindingCall(longType, 1, "params.id")
        )

        // Test Double binding
        val doubleType = ClassName("kotlin", "Double")
        assertEquals(
            "statement.bindDouble(3, tempVar)",
            typeMapping.getBindingCall(doubleType, 2, "tempVar")
        )

        // Test Boolean binding (converts to Int)
        val booleanType = ClassName("kotlin", "Boolean")
        assertEquals(
            "statement.bindInt(1, if (isActive) 1 else 0)",
            typeMapping.getBindingCall(booleanType, 0, "isActive")
        )

        // Test String binding (default case)
        val stringType = ClassName("kotlin", "String")
        assertEquals(
            "statement.bindText(1, name)",
            typeMapping.getBindingCall(stringType, 0, "name")
        )

        // Test ByteArray binding
        val byteArrayType = ClassName("kotlin", "ByteArray")
        assertEquals(
            "statement.bindBlob(1, data)",
            typeMapping.getBindingCall(byteArrayType, 0, "data")
        )
    }

    @Test
    @DisplayName("Test getter calls for standard Kotlin types")
    fun testGetterCallsForStandardTypes() {
        // Test Long getter
        val longType = ClassName("kotlin", "Long")
        assertEquals(
            "stmt.getLong(0)",
            typeMapping.getGetterCall(longType, 0)
        )

        // Test Int getter
        val intType = ClassName("kotlin", "Int")
        assertEquals(
            "stmt.getInt(2)",
            typeMapping.getGetterCall(intType, 2)
        )

        // Test Boolean getter (converts from Int)
        val booleanType = ClassName("kotlin", "Boolean")
        assertEquals(
            "stmt.getInt(1) != 0",
            typeMapping.getGetterCall(booleanType, 1)
        )

        // Test String getter
        val stringType = ClassName("kotlin", "String")
        assertEquals(
            "stmt.getText(3)",
            typeMapping.getGetterCall(stringType, 3)
        )

        // Test ByteArray getter
        val byteArrayType = ClassName("kotlin", "ByteArray")
        assertEquals(
            "stmt.getBlob(0)",
            typeMapping.getGetterCall(byteArrayType, 0)
        )
    }

    @Test
    @DisplayName("Test collection binding call")
    fun testCollectionBindingCall() {
        assertEquals(
            "statement.bindText(1, params.tags.jsonEncodeToSqlite())",
            typeMapping.getCollectionBindingCall(0, "tags")
        )

        assertEquals(
            "statement.bindText(3, params.categories.jsonEncodeToSqlite())",
            typeMapping.getCollectionBindingCall(2, "categories")
        )
    }

    @Test
    @DisplayName("Test unknown type defaults to String/Text")
    fun testUnknownTypeDefaults() {
        val customType = ClassName("com.example", "CustomType")
        
        // Unknown types should default to text binding
        assertEquals(
            "statement.bindText(1, value)",
            typeMapping.getBindingCall(customType, 0, "value")
        )

        // Unknown types should default to text getter
        assertEquals(
            "stmt.getText(0)",
            typeMapping.getGetterCall(customType, 0)
        )
    }

    @Test
    @DisplayName("Test standard type checking")
    fun testStandardTypeChecking() {
        // Standard types should be recognized
        assertEquals(true, typeMapping.isStandardKotlinType("kotlin.Int"))
        assertEquals(true, typeMapping.isStandardKotlinType("kotlin.Long"))
        assertEquals(true, typeMapping.isStandardKotlinType("kotlin.String"))
        assertEquals(true, typeMapping.isStandardKotlinType("kotlin.Boolean"))
        assertEquals(true, typeMapping.isStandardKotlinType("kotlin.ByteArray"))

        // Nullable types should also be recognized
        assertEquals(true, typeMapping.isStandardKotlinType("kotlin.Int?"))
        assertEquals(true, typeMapping.isStandardKotlinType("kotlin.String?"))

        // Non-standard types should not be recognized
        assertEquals(false, typeMapping.isStandardKotlinType("com.example.CustomType"))
        assertEquals(false, typeMapping.isStandardKotlinType("java.time.LocalDateTime"))
    }

    @Test
    @DisplayName("Test parameter index conversion (0-based to 1-based)")
    fun testParameterIndexConversion() {
        val intType = ClassName("kotlin", "Int")
        
        // Index 0 should become 1 in SQLite
        assertEquals(
            "statement.bindInt(1, value)",
            typeMapping.getBindingCall(intType, 0, "value")
        )

        // Index 5 should become 6 in SQLite
        assertEquals(
            "statement.bindInt(6, value)",
            typeMapping.getBindingCall(intType, 5, "value")
        )
    }
}
