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
package dev.goquick.sqlitenow.gradle.swift

import java.io.File
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

internal class SwiftProductSourceAssembler(
    private val databaseName: String,
    private val config: SqliteNowSwiftProductExportConfig,
    private val model: SwiftProductGenerationModel,
    private val databaseEmitter: SwiftProductDatabaseEmitter,
    private val supportEmitter: SwiftProductSupportEmitter,
    private val syncSourceEmitter: SwiftProductSyncSourceEmitter,
    private val namespaceEmitter: SwiftProductNamespaceEmitter,
) {
    private val json = Json { prettyPrint = true }

    fun generateCode(): List<File> {
        val swiftOutputDirectory = config.swiftOutputDirectory
        val sourceFiles = emitSwiftSourceFiles().sortedBy { it.relativePath }
        if (swiftOutputDirectory.exists()) {
            cleanExistingOutput(swiftOutputDirectory)
        }
        swiftOutputDirectory.mkdirs()
        val generatedFiles = sourceFiles
            .map { sourceFile ->
                swiftOutputDirectory.resolve(sourceFile.relativePath).also { outputFile ->
                    outputFile.parentFile.mkdirs()
                    outputFile.writeText(sourceFile.content)
                }
            }
        writeSourceManifest(swiftOutputDirectory, sourceFiles.map { it.relativePath })
        return generatedFiles
    }

    private fun cleanExistingOutput(swiftOutputDirectory: File) {
        if (swiftOutputDirectory.listFiles().orEmpty().isEmpty()) {
            return
        }
        val manifest = sourceManifest(swiftOutputDirectory)
        val manifestGeneratedFiles = readOwnedGeneratedFiles(manifest)
        if (manifestGeneratedFiles != null) {
            manifestGeneratedFiles.forEach { relativePath ->
                val file = swiftOutputDirectory.resolve(relativePath).normalize()
                require(file.isDescendantOrEqual(swiftOutputDirectory)) {
                    "Refusing to clean Swift product output outside output directory: ${file.absolutePath}"
                }
                if (file.isFile) {
                    check(file.delete()) { "Failed to delete stale Swift product output: ${file.absolutePath}" }
                }
            }
            deleteEmptyDirectories(swiftOutputDirectory)
            return
        }

        require(isSafeUnownedGeneratedDirectory(swiftOutputDirectory)) {
            "Refusing to clean non-empty Swift product output without SQLiteNow source metadata: " +
                swiftOutputDirectory.absolutePath
        }
        swiftOutputDirectory
            .walkBottomUp()
            .filter { it.isFile && it.extension == "swift" }
            .forEach { file ->
                check(file.delete()) { "Failed to delete stale Swift product output: ${file.absolutePath}" }
            }
        deleteEmptyDirectories(swiftOutputDirectory)
    }

    private fun readOwnedGeneratedFiles(manifest: File): List<String>? {
        if (!manifest.isFile) {
            return null
        }
        val manifestJson = runCatching { json.parseToJsonElement(manifest.readText()).jsonObject }.getOrNull()
            ?: error("Invalid SQLiteNow Swift source manifest: ${manifest.absolutePath}")
        val manifestDatabaseName = manifestJson["databaseName"]?.jsonPrimitive?.content
        val manifestSwiftModuleName = manifestJson["swiftModuleName"]?.jsonPrimitive?.content
        require(manifestDatabaseName == databaseName && manifestSwiftModuleName == config.swiftModuleName) {
            "Refusing to clean Swift product output owned by a different SQLiteNow source target: " +
                manifest.absolutePath
        }
        return manifestJson["generatedSwiftFiles"]
            ?.jsonArray
            ?.map { it.jsonPrimitive.content }
            ?: emptyList()
    }

    private fun writeSourceManifest(
        swiftOutputDirectory: File,
        generatedSwiftFiles: List<String>,
    ) {
        val manifest = sourceManifest(swiftOutputDirectory)
        manifest.parentFile.mkdirs()
        manifest.writeText(
            json.encodeToString(
                buildJsonObject {
                    put("manifestVersion", 1)
                    put("databaseName", databaseName)
                    put("swiftModuleName", config.swiftModuleName)
                    put("runtimeMode", config.runtimeMode.id)
                    put(
                        "generatedSwiftFiles",
                        buildJsonArray {
                            generatedSwiftFiles.forEach { add(it) }
                        },
                    )
                }
            ) + "\n"
        )
    }

    private fun sourceManifest(swiftOutputDirectory: File): File =
        swiftOutputDirectory.resolve(".sqlitenow/source-manifest.json")

    private fun deleteEmptyDirectories(swiftOutputDirectory: File) {
        swiftOutputDirectory
            .walkBottomUp()
            .filter {
                it.isDirectory &&
                    it != swiftOutputDirectory &&
                    it.name != ".sqlitenow" &&
                    it.listFiles().orEmpty().isEmpty()
            }
            .forEach { directory ->
                check(directory.delete()) { "Failed to delete stale Swift product output directory: ${directory.absolutePath}" }
            }
    }

    private fun isSafeUnownedGeneratedDirectory(swiftOutputDirectory: File): Boolean {
        val segments = swiftOutputDirectory.toPath().normalize().map { it.toString().lowercase() }.toList()
        return segments.any { segment ->
            segment == "build" ||
                segment == ".build" ||
                segment == "sqlitenowgenerated" ||
                "generated" in segment
        }
    }

    private fun emitSwiftSourceFiles(): List<SwiftProductSourceFile> =
        buildList {
            add(swiftSourceFile("$databaseName.swift") { databaseEmitter.emit(this) })
            if (config.emitSupportSources) {
                add(swiftSourceFile("SQLiteNowSupport.swift") { supportEmitter.emit(this) })
            }
            if (config.runtimeMode == SwiftProductRuntimeMode.SYNC && config.emitSupportSources) {
                add(swiftSourceFile("SQLiteNowSyncSupport.swift") { syncSourceEmitter.emitSyncTypes(this) })
            }
            val emittedResultNames = mutableSetOf<String>()
            model.namespaces.forEach { namespace ->
                add(swiftSourceFile("${namespace.swiftNamespaceTypeName("Queries")}.swift") {
                    namespaceEmitter.emit(this, namespace, emittedResultNames)
                })
            }
        }

    private fun swiftSourceFile(
        relativePath: String,
        emitBody: SwiftWriter.() -> Unit,
    ): SwiftProductSourceFile =
        SwiftProductSourceFile(
            relativePath = relativePath,
            content = SwiftWriter().apply {
                line("@preconcurrency import ${config.runtimeModuleName}")
                if (!config.emitSupportSources) {
                    if (config.runtimeMode == SwiftProductRuntimeMode.SYNC) {
                        line("@_exported import SQLiteNowSyncSupport")
                    } else {
                        line("@_exported import SQLiteNowCoreSupport")
                    }
                }
                line("import Foundation")
                line()
                emitBody()
            }.toString(),
        )
}

private fun File.normalize(): File = toPath().normalize().toFile()

private fun File.isDescendantOrEqual(parent: File): Boolean =
    canonicalFile.toPath().startsWith(parent.canonicalFile.toPath())

internal fun SwiftWriter.emitSwiftStringArrayElement(value: String, last: Boolean) {
    emitSwiftMultilineStringLiteral(value, suffix = if (last) "" else ",")
}

internal fun SwiftWriter.emitSwiftMultilineStringLiteral(value: String, suffix: String = "") {
    val delimiter = value.swiftRawMultilineDelimiter()
    line(delimiter.open)
    value.trim().lines().forEach { line(it) }
    line("${delimiter.close}$suffix")
}

internal fun String.swiftRawMultilineDelimiter(): SwiftRawMultilineDelimiter {
    val value = trim()
    var hashCount = 1
    while (
        value.contains("\"\"\"" + "#".repeat(hashCount)) ||
        value.contains("\\" + "#".repeat(hashCount) + "(")
    ) {
        hashCount++
    }
    val hashes = "#".repeat(hashCount)
    return SwiftRawMultilineDelimiter(open = "$hashes\"\"\"", close = "\"\"\"$hashes")
}

internal fun Collection<String>.toSwiftArrayLiteral(): String =
    joinToString(prefix = "[", postfix = "]") { it.toSwiftStringLiteral() }

internal data class SwiftProductSourceFile(
    val relativePath: String,
    val content: String,
)

internal data class SwiftRawMultilineDelimiter(
    val open: String,
    val close: String,
)
