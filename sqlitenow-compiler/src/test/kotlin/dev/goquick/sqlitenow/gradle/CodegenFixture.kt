package dev.goquick.sqlitenow.gradle

import java.io.File
import kotlin.io.path.createTempDirectory

internal class CodegenFixture private constructor(
    val root: File,
    private val dbName: String,
    private val packageName: String,
    private val schemaDir: File,
    private val queriesDir: File,
    private val outDir: File,
) {
    fun writeSchema(fileName: String, sql: String) {
        schemaDir.resolve(fileName).writeText(sql.trimIndent())
    }

    fun writeQuery(namespace: String, fileName: String, sql: String) {
        queriesDir.resolve(namespace).apply { mkdirs() }
            .resolve(fileName)
            .writeText(sql.trimIndent())
    }

    fun generate(debug: Boolean = false, schemaDatabaseFile: File? = null) {
        generateDatabaseFiles(
            dbName = dbName,
            sqlDir = root,
            packageName = packageName,
            outDir = outDir,
            schemaDatabaseFile = schemaDatabaseFile,
            debug = debug,
        )
    }

    fun generatedTextContaining(fileNamePart: String): String =
        generatedFileContaining(fileNamePart).readText()

    fun generatedFiles(): List<File> =
        outDir.walkTopDown().filter { it.isFile && it.extension == "kt" }.toList()

    private fun generatedFileContaining(fileNamePart: String): File =
        outDir.walkTopDown().first { it.name.contains(fileNamePart) && it.extension == "kt" }

    companion object {
        fun create(
            prefix: String,
            dbName: String = "TestDb",
            packageName: String = "dev.test",
        ): CodegenFixture {
            val root = createTempDirectory(prefix = prefix).toFile()
            return CodegenFixture(
                root = root,
                dbName = dbName,
                packageName = packageName,
                schemaDir = root.resolve("schema").apply { mkdirs() },
                queriesDir = root.resolve("queries").apply { mkdirs() },
                outDir = root.resolve("out").apply { mkdirs() },
            )
        }
    }
}
