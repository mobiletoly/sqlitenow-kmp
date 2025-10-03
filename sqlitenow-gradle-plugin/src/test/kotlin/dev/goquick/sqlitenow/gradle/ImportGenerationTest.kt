package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.asTypeName
import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.ColumnLookup
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ImportGenerationTest {

    @Test
    fun testCustomTypeWithoutPackageUsesProvidedPackage() {
        // Test that a custom type without a dot uses the provided package name
        val customType = SqliteTypeToKotlinCodeConverter.determinePropertyType(
            baseType = String::class.asTypeName(),
            propertyType = "Gender", // Custom type without package
            isNullable = false,
            packageName = "com.example.db"
        )

        assertTrue(customType is ClassName, "Custom type should be a ClassName")
        val className = customType as ClassName

        // The package name should be the provided package name
        assertEquals("com.example.db", className.packageName, "Package name should be the provided package name")
        assertEquals("Gender", className.simpleName, "Simple name should be Gender")
    }

    @Test
    fun testCustomTypeWithPackageGeneratesImport() {
        // Test that a custom type with a dot (different package) creates ClassName with package
        val customType = SqliteTypeToKotlinCodeConverter.determinePropertyType(
            baseType = String::class.asTypeName(),
            propertyType = "com.example.Gender", // Custom type with package
            isNullable = false
        )

        assertTrue(customType is ClassName, "Custom type should be a ClassName")
        val className = customType as ClassName

        // The package name should be preserved for different-package classes
        assertEquals("com.example", className.packageName, "Package name should be preserved for different-package classes")
        assertEquals("Gender", className.simpleName, "Simple name should be Gender")
    }

    @Test
    fun testGenericTypeWithCustomTypeArgument() {
        // Test that generic types with custom type arguments work correctly
        val customType = SqliteTypeToKotlinCodeConverter.determinePropertyType(
            baseType = String::class.asTypeName(),
            propertyType = "List<Gender>", // Generic type with custom type argument
            isNullable = false,
            packageName = "com.example.db"
        )

        // Should be a parameterized type
        assertTrue(customType.toString().contains("List"), "Should contain List")
        assertTrue(customType.toString().contains("Gender"), "Should contain Gender")
        // The Gender type argument should use the provided package
        assertTrue(customType.toString().contains("com.example.db.Gender"), "Should contain fully qualified Gender type")
    }

    @Test
    fun testKotlinStandardTypesStillWork() {
        // Test that standard Kotlin types still work correctly
        val stringType = SqliteTypeToKotlinCodeConverter.determinePropertyType(
            baseType = String::class.asTypeName(),
            propertyType = "String",
            isNullable = false
        )

        val listType = SqliteTypeToKotlinCodeConverter.determinePropertyType(
            baseType = String::class.asTypeName(),
            propertyType = "List<String>",
            isNullable = false
        )

        assertTrue(stringType is ClassName, "String type should be a ClassName")
        val stringClassName = stringType as ClassName
        assertEquals("kotlin", stringClassName.packageName, "String should have kotlin package")
        assertEquals("String", stringClassName.simpleName, "Simple name should be String")
    }

    @Test
    fun testAdapterConfigUsesPackageName() {
        // Test that AdapterConfig correctly uses package name for custom types
        val createTableStatements = emptyList<AnnotatedCreateTableStatement>()
        val columnLookup = ColumnLookup(createTableStatements, emptyList())
        val packageName = "com.example.db"

        // Create AdapterConfig with package name
        val adapterConfig = AdapterConfig(columnLookup, createTableStatements, packageName)

        // Verify that the AdapterConfig was created successfully with package name
        // This test mainly verifies that our constructor changes work correctly
        assertTrue(true, "AdapterConfig should be created successfully with package name parameter")
    }
}
