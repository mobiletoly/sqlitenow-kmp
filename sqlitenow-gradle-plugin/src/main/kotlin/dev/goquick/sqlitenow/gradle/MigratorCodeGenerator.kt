package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.TypeSpec
import dev.goquick.sqlitenow.gradle.sqlite.SqlSingleStatement
import dev.goquick.sqlitenow.gradle.sqlite.translateSqliteStatementToKotlin
import java.io.File
import org.gradle.api.logging.Logger
import org.gradle.api.logging.Logging

/**
 * Generates Kotlin code for executing SQL migration statements.
 * The generated code will include a function that accepts an androidx.sqlite.SQLiteConnection
 * parameter and executes all the SQL statements from the migrations.
 */
internal class MigratorCodeGenerator(
    private val schemaInspector: SchemaInspector,
    private val sqlBatchInspector: SQLBatchInspector,
    private val migrationInspector: MigrationInspector,
    private val packageName: String,
    private val outputDir: File,
) {
    // Logger for this class
    private val logger: Logger = Logging.getLogger(MigratorCodeGenerator::class.java)

    /**
     * Generates the applyMigration function that implements the DatabaseMigrations interface.
     * This function applies migrations based on the current database version.
     */
    private fun generateApplyMigrationFunction(): FunSpec {
        // Create the function builder
        val functionBuilder = FunSpec.builder("applyMigration")
            .addModifiers(KModifier.OVERRIDE)
            .addAnnotation(AnnotationSpec.builder(Suppress::class)
                .addMember("%S", "SameReturnValue")
                .build())
            .addParameter("conn", ClassName("androidx.sqlite", "SQLiteConnection"))
            .addParameter("currentVersion", Int::class)
            .returns(Int::class)

        val codeBlockBuilder = CodeBlock.builder()

        // Handle initial setup when currentVersion is -1
        codeBlockBuilder.addStatement("if (currentVersion == -1) {")
        codeBlockBuilder.addStatement("    executeAllSql(conn)")
        codeBlockBuilder.addStatement("    return ${migrationInspector.latestVersion}")
        codeBlockBuilder.addStatement("}")
        codeBlockBuilder.add("\n")

        // Generate incremental migration calls for each version
        migrationInspector.sqlStatements.keys.sorted().forEach { version ->
            codeBlockBuilder.addStatement("if (currentVersion < $version) {")
            codeBlockBuilder.addStatement("    migrateToVersion$version(conn)")
            codeBlockBuilder.addStatement("}")
            codeBlockBuilder.add("\n")
        }

        // Return the latest version
        codeBlockBuilder.addStatement("return ${migrationInspector.latestVersion}")

        return functionBuilder
            .addCode(codeBlockBuilder.build())
            .build()
    }

    /** Generates a function that executes all SQL statements from a specific version. */
    private fun generateMigrateToVersionFunction(version: Int, statements: List<SqlSingleStatement>): FunSpec {
        // Create the function builder
        val functionBuilder = FunSpec.builder("migrateToVersion$version")
            .addModifiers(KModifier.PRIVATE)
            .addParameter("conn", ClassName("androidx.sqlite", "SQLiteConnection"))

        // Add the function body
        val codeBlockBuilder = CodeBlock.builder()
            .addStatement("// Execute SQL statements for version $version")

        // Add each SQL statement
        addSqlStatementsToCodeBlock(codeBlockBuilder, statements)

        // Add the code block to the function
        functionBuilder.addCode(codeBlockBuilder.build())

        return functionBuilder.build()
    }

    /** Helper function to add SQL statement execution code to a CodeBlock. */
    private fun addSqlStatementsToCodeBlock(codeBlockBuilder: CodeBlock.Builder, statements: List<SqlSingleStatement>) {
        statements.forEach { statement ->
            // Convert the SQL statement to a Kotlin string with proper formatting
            val formattedSql = translateSqliteStatementToKotlin(statement.sql)

            // Add the statement to execute the SQL
            // Handle dollar signs in SQL by using the $$ syntax in the generated code
            if (formattedSql.contains("$")) {
                // Use $$ syntax for SQL containing dollar signs
                codeBlockBuilder.addStatement("conn.execSQL($$\"\"\"$formattedSql\"\"\".trimMargin())")
            } else {
                // Regular case without dollar signs
                codeBlockBuilder.addStatement("conn.execSQL(\"\"\"$formattedSql\"\"\".trimMargin())")
            }
        }
    }

    /** Generates a function that executes all SQL statements from the schema and batch inspector. */
    private fun generateExecuteSqlFunction(): FunSpec {
        // Create the function builder
        val functionBuilder = FunSpec.builder("executeAllSql")
            .addModifiers(KModifier.PRIVATE)
            .addParameter("conn", ClassName("androidx.sqlite", "SQLiteConnection"))

        // Add the function body
        val codeBlockBuilder = CodeBlock.builder()
            .addStatement("// Execute all SQL statements from the schema and batch inspector")

        // Combine statements from both schema inspector and batch inspector
        val allStatements = schemaInspector.sqlStatements + sqlBatchInspector.sqlStatements

        // Add each SQL statement
        addSqlStatementsToCodeBlock(codeBlockBuilder, allStatements)

        // Add the code block to the function
        functionBuilder.addCode(codeBlockBuilder.build())

        return functionBuilder.build()
    }

    fun generateCode(className: String = "VersionBasedDatabaseMigrations"): FileSpec {
        // Create the output directory if it doesn't exist
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw RuntimeException("Failed to create output directory: ${outputDir.absolutePath}")
        }

        // Create a class to hold the migration functions
        val classBuilder = TypeSpec.classBuilder(className)
            // Make the class public so it can be accessed from other modules
            .addSuperinterface(ClassName("dev.goquick.sqlitenow.core", "DatabaseMigrations"))
            // Add an empty constructor
            .addFunction(
                FunSpec.constructorBuilder().build()
            )

        // Add the applyMigration function that implements the DatabaseMigrations interface
        val applyMigrationFunction = generateApplyMigrationFunction()
        classBuilder.addFunction(applyMigrationFunction)

        // Add individual migration functions for each version
        migrationInspector.sqlStatements.forEach { (version, statements) ->
            val versionFunction = generateMigrateToVersionFunction(version, statements)
            classBuilder.addFunction(versionFunction)
        }

        // Add the function to execute all SQL statements
        val executeSqlFunction = generateExecuteSqlFunction()
        classBuilder.addFunction(executeSqlFunction)

        // Build the file spec
        val fileSpec = FileSpec.builder(packageName, className)
            .addType(classBuilder.build())
            .addImport("androidx.sqlite", "SQLiteConnection")
            .addImport("androidx.sqlite", "execSQL")
            .addImport("dev.goquick.sqlitenow.core", "DatabaseMigrations")
            .build()

        // Write the file
        fileSpec.writeTo(outputDir)

        return fileSpec
    }
}
