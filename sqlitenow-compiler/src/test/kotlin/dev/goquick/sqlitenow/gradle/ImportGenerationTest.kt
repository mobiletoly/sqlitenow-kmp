package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.asTypeName
import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.ColumnLookup
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ImportGenerationTest {

    @TestFactory
    fun propertyTypeClassNamePackageCases(): List<DynamicTest> = listOf(
        ClassNamePackageCase(
            displayName = "custom type without package uses provided package",
            propertyType = "Gender",
            packageName = "com.example.db",
            expectedPackageName = "com.example.db",
            expectedSimpleName = "Gender",
        ),
        ClassNamePackageCase(
            displayName = "custom type with package preserves that package",
            propertyType = "com.example.Gender",
            expectedPackageName = "com.example",
            expectedSimpleName = "Gender",
        ),
        ClassNamePackageCase(
            displayName = "Kotlin standard type resolves to kotlin package",
            propertyType = "String",
            expectedPackageName = "kotlin",
            expectedSimpleName = "String",
        ),
    ).map { case ->
        DynamicTest.dynamicTest(case.displayName) {
            val customType = SqliteTypeToKotlinCodeConverter.determinePropertyType(
                baseType = String::class.asTypeName(),
                propertyType = case.propertyType,
                isNullable = false,
                packageName = case.packageName,
            )

            assertTrue(customType is ClassName, "Property type should be a ClassName")
            assertEquals(case.expectedPackageName, customType.packageName)
            assertEquals(case.expectedSimpleName, customType.simpleName)
        }
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

    private data class ClassNamePackageCase(
        val displayName: String,
        val propertyType: String,
        val packageName: String = "",
        val expectedPackageName: String,
        val expectedSimpleName: String,
    )
}
