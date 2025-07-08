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
    private val debug: Boolean = false,
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
            .addModifiers(KModifier.OVERRIDE, KModifier.SUSPEND)
            .addAnnotation(AnnotationSpec.builder(Suppress::class)
                .addMember("%S", "SameReturnValue")
                .build())
            .addParameter("conn", ClassName("dev.goquick.sqlitenow.core", "SafeSQLiteConnection"))
            .addParameter("currentVersion", Int::class)
            .returns(Int::class)

        val codeBlockBuilder = CodeBlock.builder()

        // Wrap entire migration logic with a single withContext
        if (debug) {
            codeBlockBuilder.add("return conn.withContextAndTrace {\n")
        } else {
            codeBlockBuilder.add("return withContext(conn.dispatcher) {\n")
        }

        // Handle initial setup when currentVersion is -1
        codeBlockBuilder.add("    if (currentVersion == -1) {\n")
        codeBlockBuilder.add("        executeAllSql(conn)\n")
        if (debug) {
            codeBlockBuilder.add("        return@withContextAndTrace ${migrationInspector.latestVersion}\n")
        } else {
            codeBlockBuilder.add("        return@withContext ${migrationInspector.latestVersion}\n")
        }
        codeBlockBuilder.add("    }\n")
        codeBlockBuilder.add("\n")

        // Generate incremental migration calls for each version
        migrationInspector.sqlStatements.keys.sorted().forEach { version ->
            codeBlockBuilder.add("    if (currentVersion < $version) {\n")
            codeBlockBuilder.add("        migrateToVersion$version(conn)\n")
            codeBlockBuilder.add("    }\n")
            codeBlockBuilder.add("\n")
        }

        // Return the latest version
        codeBlockBuilder.add("    ${migrationInspector.latestVersion}\n")
        codeBlockBuilder.add("}\n")

        return functionBuilder
            .addCode(codeBlockBuilder.build())
            .build()
    }

    /** Generates a function that executes all SQL statements from a specific version. */
    private fun generateMigrateToVersionFunction(version: Int, statements: List<SqlSingleStatement>): FunSpec {
        // Create the function builder
        val functionBuilder = FunSpec.builder("migrateToVersion$version")
            .addModifiers(KModifier.PRIVATE)
            .addParameter("conn", ClassName("dev.goquick.sqlitenow.core", "SafeSQLiteConnection"))

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

            // Add the statement to execute the SQL (no individual withLock needed)
            // Handle dollar signs in SQL by using the $$ syntax in the generated code
            if (formattedSql.contains("$")) {
                // Use $$ syntax for SQL containing dollar signs
                codeBlockBuilder.addStatement("conn.ref.execSQL($$\"\"\"$formattedSql\"\"\".trimMargin())")
            } else {
                // Regular case without dollar signs
                codeBlockBuilder.addStatement("conn.ref.execSQL(\"\"\"$formattedSql\"\"\".trimMargin())")
            }
        }
    }

    /** Generates a function that executes all SQL statements from the schema and batch inspector. */
    private fun generateExecuteSqlFunction(): FunSpec {
        // Create the function builder
        val functionBuilder = FunSpec.builder("executeAllSql")
            .addModifiers(KModifier.PRIVATE)
            .addParameter("conn", ClassName("dev.goquick.sqlitenow.core", "SafeSQLiteConnection"))

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
        val fileSpecBuilder = FileSpec.builder(packageName, className)
            .addType(classBuilder.build())
            .addImport("dev.goquick.sqlitenow.core", "SafeSQLiteConnection")
            .addImport("androidx.sqlite", "execSQL")
            .addImport("dev.goquick.sqlitenow.core", "DatabaseMigrations")

        if (debug) {
            // No additional import needed for conn.withContextAndTrace
        } else {
            fileSpecBuilder.addImport("kotlinx.coroutines", "withContext")
        }

        val fileSpec = fileSpecBuilder.build()

        // Write the file
        fileSpec.writeTo(outputDir)

        return fileSpec
    }
}
