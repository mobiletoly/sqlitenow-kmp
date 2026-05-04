package dev.goquick.sqlitenow.gradle.compiler

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.zip.ZipFile
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class SqliteNowCompilerArtifactTest {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun compilerArtifactRunsWithoutGradlePlugin() {
        val compilerJar = File(
            checkNotNull(System.getProperty("sqlitenow.compilerJar")) {
                "sqlitenow.compilerJar system property must point at the runnable compiler jar"
            }
        )
        assertTrue(compilerJar.isFile, "Expected compiler jar at ${compilerJar.absolutePath}")

        val sqlDir = tempDir.resolve("sql/ArtifactDatabase")
        sqlDir.resolve("schema/person.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    CREATE TABLE person (
                        id INTEGER PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL
                    );
                """.trimIndent()
            )
        }
        sqlDir.resolve("queries/person/selectById.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    SELECT *
                    FROM person
                    WHERE id = :id;
                """.trimIndent()
            )
        }

        val outputDir = tempDir.resolve("generated")
        val requestFile = tempDir.resolve("request.json").apply {
            writeText(
                buildJsonObject {
                    put("databaseName", "ArtifactDatabase")
                    put("sqlDirectory", sqlDir.absolutePath)
                    put("packageName", "com.test.artifact")
                    put("outputDirectory", outputDir.absolutePath)
                    put("schemaDatabaseFile", JsonNull)
                    put("debug", false)
                    put("oversqlite", false)
                }.toString()
            )
        }

        val process = ProcessBuilder(
            javaExecutable(),
            "-jar",
            compilerJar.absolutePath,
            "--request",
            requestFile.absolutePath,
        )
            .directory(tempDir)
            .start()
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), "Compiler artifact process timed out")

        val stdout = process.inputStream.bufferedReader().readText()
        val stderr = process.errorStream.bufferedReader().readText()
        assertEquals(0, process.exitValue(), "stdout:\n$stdout\nstderr:\n$stderr")

        val response = Json.parseToJsonElement(stdout).jsonObject
        assertEquals(true, response.getValue("success").jsonPrimitive.boolean)
        assertEquals(1, response.getValue("contractVersion").jsonPrimitive.content.toInt())
        assertTrue(response.getValue("warnings").jsonArray.isEmpty())
        assertTrue(
            response.getValue("generatedFiles").jsonArray.any {
                it.jsonPrimitive.content.endsWith("/ArtifactDatabase.kt")
            },
            "Response should list generated database source. stdout:\n$stdout"
        )
        assertTrue(
            outputDir.resolve("com/test/artifact/ArtifactDatabase.kt").isFile,
            "Compiler artifact should write Kotlin output without applying the Gradle plugin"
        )
        assertFalse(
            tempDir.resolve("null").exists(),
            "JSON null for schemaDatabaseFile must not create a file named 'null'"
        )
        assertNotNull(response.getValue("java").jsonObject["minimumMajorVersion"])
    }

    @Test
    fun compilerArtifactDoesNotBundleGradleHostRuntime() {
        val compilerJar = File(
            checkNotNull(System.getProperty("sqlitenow.compilerJar")) {
                "sqlitenow.compilerJar system property must point at the runnable compiler jar"
            }
        )
        val forbiddenEntries = listOf(
            "org/gradle/",
            "groovy/",
            "groovyjarjar",
            "org/codehaus/groovy/",
            "META-INF/gradle-plugins/",
            "gradle-wrapper.jar",
            "org/jetbrains/kotlin/gradle/",
        )

        val matches = ZipFile(compilerJar).use { zip ->
            zip.entries()
                .asSequence()
                .map { it.name }
                .filter { entry -> forbiddenEntries.any { forbidden -> entry.contains(forbidden) } }
                .take(20)
                .toList()
        }

        assertTrue(
            matches.isEmpty(),
            "Compiler artifact must not contain Gradle/Groovy host runtime entries. Matches: $matches"
        )
    }

    private fun javaExecutable(): String =
        File(System.getProperty("java.home"), "bin/java").absolutePath
}
