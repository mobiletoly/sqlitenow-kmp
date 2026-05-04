/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.gradle.compiler

import java.io.File
import java.io.PrintStream
import kotlin.system.exitProcess
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

private const val CONTRACT_VERSION = 1
private const val MINIMUM_JAVA_MAJOR_VERSION = 17

fun main(args: Array<String>) {
    exitProcess(SqliteNowCompilerCli.run(args))
}

object SqliteNowCompilerCli {
    fun run(
        args: Array<String>,
        out: PrintStream = System.out,
        err: PrintStream = System.err,
    ): Int {
        return try {
            val javaMajorVersion = currentJavaMajorVersion()
            if (args.size == 1 && args[0] == "--contract") {
                out.println(Json.encodeToString(JsonElement.serializer(), contractResponse(javaMajorVersion)))
                return 0
            }
            if (javaMajorVersion < MINIMUM_JAVA_MAJOR_VERSION) {
                out.println(
                    Json.encodeToString(
                        JsonElement.serializer(),
                        failureResponse(
                            type = "UnsupportedJavaVersion",
                            message = "SQLiteNow compiler requires Java $MINIMUM_JAVA_MAJOR_VERSION or newer.",
                            javaMajorVersion = javaMajorVersion,
                        )
                    )
                )
                return 1
            }

            val requestFile = requestFileFromArgs(args)
            val input = parseCompilerInput(requestFile)
            val result = compileSqliteNowDatabase(input)
            out.println(Json.encodeToString(JsonElement.serializer(), successResponse(result, javaMajorVersion)))
            0
        } catch (e: Throwable) {
            out.println(
                Json.encodeToString(
                    JsonElement.serializer(),
                    failureResponse(
                        type = e::class.qualifiedName ?: e::class.simpleName ?: "Throwable",
                        message = e.message ?: "Unknown SQLiteNow compiler failure.",
                        javaMajorVersion = currentJavaMajorVersionOrNull(),
                    )
                )
            )
            err.println(e.message ?: e::class.qualifiedName ?: "SQLiteNow compiler failure")
            1
        }
    }

    private fun requestFileFromArgs(args: Array<String>): File {
        require(args.size == 2 && args[0] == "--request") {
            "Usage: java -jar sqlitenow-compiler.jar --request <request.json>"
        }
        return File(args[1])
    }

    private fun parseCompilerInput(requestFile: File): SqliteNowCompilerInput {
        val request = Json.parseToJsonElement(requestFile.readText()).jsonObject
        return SqliteNowCompilerInput(
            databaseName = request.requiredString("databaseName"),
            sqlDirectory = File(request.requiredString("sqlDirectory")),
            packageName = request.requiredString("packageName"),
            outputDirectory = File(request.requiredString("outputDirectory")),
            schemaDatabaseFile = request.optionalString("schemaDatabaseFile")?.let(::File),
            debug = request.optionalBoolean("debug") ?: false,
            oversqlite = request.optionalBoolean("oversqlite") ?: false,
            backend = request.optionalBackend("backend") ?: SqliteNowCompilerBackend.KOTLIN,
        )
    }

    private fun successResponse(
        result: SqliteNowCompilerResult,
        javaMajorVersion: Int,
    ): JsonObject = buildJsonObject {
        put("success", true)
        put("contractVersion", CONTRACT_VERSION)
        put("java", javaResponse(javaMajorVersion))
        put(
            "generatedFiles",
            buildJsonArray {
                result.generatedFiles.forEach { add(JsonPrimitive(it.absoluteFile.invariantSeparatorsPath)) }
            }
        )
        put(
            "warnings",
            buildJsonArray {
                result.warnings.forEach { add(JsonPrimitive(it)) }
            }
        )
        put("diagnostics", diagnosticsResponse(result.diagnostics))
    }

    private fun failureResponse(
        type: String,
        message: String,
        javaMajorVersion: Int?,
    ): JsonObject = buildJsonObject {
        put("success", false)
        put("contractVersion", CONTRACT_VERSION)
        put("java", javaResponse(javaMajorVersion))
        put("generatedFiles", JsonArray(emptyList()))
        put("warnings", JsonArray(emptyList()))
        put("diagnostics", JsonArray(emptyList()))
        put(
            "failure",
            buildJsonObject {
                put("type", type)
                put("message", message)
            }
        )
    }

    private fun contractResponse(javaMajorVersion: Int): JsonObject = buildJsonObject {
        put("success", true)
        put("contractVersion", CONTRACT_VERSION)
        put("java", javaResponse(javaMajorVersion))
        put("request", buildJsonObject {
            put("databaseName", "string")
            put("sqlDirectory", "absolute-or-relative-path")
            put("packageName", "string")
            put("outputDirectory", "absolute-or-relative-path")
            put("schemaDatabaseFile", "optional-path-or-null")
            put("debug", "optional-boolean")
            put("oversqlite", "optional-boolean")
            put("backend", "optional-string-kotlin-or-dart")
        })
        put("response", buildJsonObject {
            put("success", "boolean")
            put("contractVersion", "integer")
            put("java", "object")
            put("generatedFiles", "string-array")
            put("warnings", "string-array")
            put("diagnostics", "object-array")
            put("failure", "object-when-success-is-false")
        })
    }

    private fun diagnosticsResponse(diagnostics: List<SqliteNowCompilerDiagnostic>): JsonArray =
        buildJsonArray {
            diagnostics.forEach { diagnostic ->
                add(
                    buildJsonObject {
                        put("severity", diagnostic.severity.name)
                        put("message", diagnostic.message)
                    }
                )
            }
        }

    private fun javaResponse(javaMajorVersion: Int?): JsonObject = buildJsonObject {
        put("minimumMajorVersion", MINIMUM_JAVA_MAJOR_VERSION)
        if (javaMajorVersion != null) {
            put("majorVersion", javaMajorVersion)
        }
        put("version", System.getProperty("java.version"))
    }

    private fun currentJavaMajorVersionOrNull(): Int? =
        runCatching { currentJavaMajorVersion() }.getOrNull()

    internal fun currentJavaMajorVersion(
        specificationVersion: String = System.getProperty("java.specification.version"),
    ): Int {
        return if (specificationVersion.startsWith("1.")) {
            specificationVersion.substringAfter("1.").substringBefore(".").toInt()
        } else {
            specificationVersion.substringBefore(".").toInt()
        }
    }

    private fun JsonObject.requiredString(name: String): String =
        optionalString(name) ?: error("Missing required request field '$name'.")

    private fun JsonObject.optionalString(name: String): String? =
        this[name]
            ?.takeUnless { it is JsonNull }
            ?.jsonPrimitive
            ?.content
            ?.takeIf { it.isNotBlank() }

    private fun JsonObject.optionalBoolean(name: String): Boolean? =
        this[name]
            ?.takeUnless { it is JsonNull }
            ?.jsonPrimitive
            ?.boolean

    private fun JsonObject.optionalBackend(name: String): SqliteNowCompilerBackend? =
        optionalString(name)?.let { raw ->
            when (raw.lowercase()) {
                "kotlin" -> SqliteNowCompilerBackend.KOTLIN
                "dart" -> SqliteNowCompilerBackend.DART
                else -> error("Unsupported compiler backend '$raw'. Expected 'kotlin' or 'dart'.")
            }
        }
}
