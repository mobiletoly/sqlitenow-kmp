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

import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS
import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_PACKAGE_MINIMUM_IOS
import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_PACKAGE_MINIMUM_MACOS
import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME
import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_SYNC_RUNTIME_MODULE_NAME
import dev.goquick.sqlitenow.gradle.swift.SWIFT_PACKAGE_FRAMEWORK_MODE_DYNAMIC
import dev.goquick.sqlitenow.gradle.swift.SqliteNowSwiftProductExportConfig
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageAssembler
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageAssemblerInput
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageAssemblerResult
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageCleanPolicy
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageDependency
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageDependencyKind
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageMinimumPlatforms
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageRuntimeArtifact
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageRuntimeArtifactValidationContext
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageToolsMetadata
import dev.goquick.sqlitenow.gradle.swift.SwiftProductRuntimeMode
import dev.goquick.sqlitenow.gradle.swift.parseSwiftPackageRuntimeArtifactRequest
import dev.goquick.sqlitenow.gradle.swift.putNullableString
import dev.goquick.sqlitenow.gradle.swift.requireSupportedSwiftPackageAppleTargets
import dev.goquick.sqlitenow.gradle.swift.requireSupportedSwiftPackageFrameworkMode
import dev.goquick.sqlitenow.gradle.swift.requireValidSwiftIdentifier
import dev.goquick.sqlitenow.gradle.swift.requireValidSwiftPackageName
import dev.goquick.sqlitenow.gradle.swift.requireValidSwiftPackageRuntimeArtifact
import dev.goquick.sqlitenow.gradle.swift.resolveSwiftPackageRuntimeArtifact
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
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

private const val LEGACY_CONTRACT_VERSION = 1
private const val SWIFT_PRODUCT_CONTRACT_VERSION = 2
private const val LATEST_CONTRACT_VERSION = SWIFT_PRODUCT_CONTRACT_VERSION
private const val SWIFT_PRODUCT_GENERATION_MODE = "swiftProductSource"
private const val SWIFT_PRODUCT_PACKAGE_GENERATION_MODE = "swiftProductPackage"
private const val DEFAULT_SWIFT_PACKAGE_OUTPUT_ROOT = "SQLiteNowGenerated"
private const val MINIMUM_JAVA_MAJOR_VERSION = 17
private const val UNKNOWN_GENERATOR_VERSION = "unknown"

fun main(args: Array<String>) {
    exitProcess(SqliteNowCompilerCli.run(args))
}

object SqliteNowCompilerCli {
    private data class ParsedCompilerRequest(
        val input: SqliteNowCompilerInput,
        val responseContractVersion: Int,
        val generationMode: String? = null,
        val swiftOutputDirectory: File? = null,
        val swiftPackage: ParsedSwiftProductPackageRequest? = null,
    )

    private data class ParsedSwiftProductPackageRequest(
        val assemblerInput: SwiftPackageAssemblerInput,
    )

    fun run(
        args: Array<String>,
        out: PrintStream = System.out,
        err: PrintStream = System.err,
    ): Int {
        var responseContractVersion = LEGACY_CONTRACT_VERSION
        return try {
            val javaMajorVersion = currentJavaMajorVersion()
            if (args.size == 1 && args[0] == "--contract") {
                out.println(Json.encodeToString(JsonElement.serializer(), contractResponse(javaMajorVersion)))
                return 0
            }
            if (args.size == 1 && args[0] == "--version") {
                out.println(generatorVersion())
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
                            contractVersion = responseContractVersion,
                        )
                    )
                )
                return 1
            }

            val requestFile = requestFileFromArgs(args)
            responseContractVersion = responseContractVersionForFailure(requestFile)
            val request = parseCompilerRequest(requestFile)
            responseContractVersion = request.responseContractVersion
            val result = compileSqliteNowDatabase(request.input)
            val packageResult = request.swiftPackage?.let { swiftPackage ->
                SwiftPackageAssembler.assemble(
                    swiftPackage.assemblerInput.copy(
                        syncTables = result.swiftPackageSyncTables,
                        tools = SwiftPackageToolsMetadata.discover(),
                    )
                )
            }
            out.println(Json.encodeToString(JsonElement.serializer(), successResponse(result, javaMajorVersion, request, packageResult)))
            0
        } catch (e: Throwable) {
            out.println(
                Json.encodeToString(
                    JsonElement.serializer(),
                    failureResponse(
                        type = e::class.qualifiedName ?: e::class.simpleName ?: "Throwable",
                        message = e.message ?: "Unknown SQLiteNow compiler failure.",
                        javaMajorVersion = currentJavaMajorVersionOrNull(),
                        contractVersion = responseContractVersion,
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

    private fun responseContractVersionForFailure(requestFile: File): Int =
        runCatching {
            val request = Json.parseToJsonElement(requestFile.readText()).jsonObject
            when (request.optionalInt("contractVersion")) {
                LEGACY_CONTRACT_VERSION -> LEGACY_CONTRACT_VERSION
                SWIFT_PRODUCT_CONTRACT_VERSION -> SWIFT_PRODUCT_CONTRACT_VERSION
                else -> when (request.optionalString("generationMode")) {
                    SWIFT_PRODUCT_GENERATION_MODE -> SWIFT_PRODUCT_CONTRACT_VERSION
                    SWIFT_PRODUCT_PACKAGE_GENERATION_MODE -> SWIFT_PRODUCT_CONTRACT_VERSION
                    else -> LEGACY_CONTRACT_VERSION
                }
            }
        }.getOrDefault(LEGACY_CONTRACT_VERSION)

    private fun parseCompilerRequest(requestFile: File): ParsedCompilerRequest {
        val request = Json.parseToJsonElement(requestFile.readText()).jsonObject
        return when (val generationMode = request.optionalString("generationMode")) {
            null -> parseLegacyCompilerRequest(request)
            SWIFT_PRODUCT_GENERATION_MODE -> parseSwiftProductSourceRequest(request)
            SWIFT_PRODUCT_PACKAGE_GENERATION_MODE -> parseSwiftProductPackageRequest(request, requestFile)
            else -> error(
                "Unsupported generationMode '$generationMode'. Expected '$SWIFT_PRODUCT_GENERATION_MODE' " +
                    "or '$SWIFT_PRODUCT_PACKAGE_GENERATION_MODE'."
            )
        }
    }

    private fun parseLegacyCompilerRequest(request: JsonObject): ParsedCompilerRequest {
        val contractVersion = request.optionalInt("contractVersion") ?: LEGACY_CONTRACT_VERSION
        require(contractVersion == LEGACY_CONTRACT_VERSION) {
            "Legacy Kotlin/Dart requests require contractVersion=$LEGACY_CONTRACT_VERSION, " +
                "but request specified $contractVersion."
        }

        return ParsedCompilerRequest(
            input = SqliteNowCompilerInput(
                databaseName = request.requiredString("databaseName"),
                sqlDirectory = File(request.requiredString("sqlDirectory")),
                packageName = request.requiredString("packageName"),
                outputDirectory = File(request.requiredString("outputDirectory")),
                schemaDatabaseFile = request.optionalString("schemaDatabaseFile")?.let(::File),
                debug = request.optionalBoolean("debug") ?: false,
                oversqlite = request.optionalBoolean("oversqlite") ?: false,
                backend = request.optionalBackend("backend") ?: SqliteNowCompilerBackend.KOTLIN,
            ),
            responseContractVersion = LEGACY_CONTRACT_VERSION,
        )
    }

    private fun parseSwiftProductSourceRequest(request: JsonObject): ParsedCompilerRequest {
        val contractVersion = request.optionalInt("contractVersion") ?: LEGACY_CONTRACT_VERSION
        require(contractVersion == SWIFT_PRODUCT_CONTRACT_VERSION) {
            "generationMode '$SWIFT_PRODUCT_GENERATION_MODE' requires " +
                "contractVersion=$SWIFT_PRODUCT_CONTRACT_VERSION, but request specified $contractVersion."
        }

        rejectSwiftProductLegacyFields(request, SWIFT_PRODUCT_GENERATION_MODE)

        val runtime = request.requiredString("runtime")
        val runtimeMode = SwiftProductRuntimeMode.fromId(runtime)
        val databaseName = requireValidSwiftIdentifier(request.requiredString("databaseName"), "Swift product databaseName")
        val swiftModuleName = requireValidSwiftIdentifier(request.requiredString("swiftModuleName"), "Swift product swiftModuleName")
        val swiftOutputDirectory = File(request.requiredString("swiftOutputDirectory"))
        val runtimeModuleName = request.optionalString("runtimeModuleName")
            ?: runtimeMode.defaultRuntimeModuleName()
        val validatedRuntimeModuleName = requireValidSwiftIdentifier(runtimeModuleName, "Swift product runtimeModuleName")

        return ParsedCompilerRequest(
            input = SqliteNowCompilerInput(
                databaseName = databaseName,
                sqlDirectory = File(request.requiredString("sqlDirectory")),
                packageName = request.requiredString("metadataPackageName"),
                outputDirectory = File(request.requiredString("compilerOutputDirectory")),
                schemaDatabaseFile = request.optionalString("schemaDatabaseFile")?.let(::File),
                debug = request.optionalBoolean("debug") ?: false,
                backend = SqliteNowCompilerBackend.KOTLIN,
                swiftProductExport = SqliteNowSwiftProductExportConfig(
                    swiftOutputDirectory = swiftOutputDirectory,
                    swiftModuleName = swiftModuleName,
                    runtimeModuleName = validatedRuntimeModuleName,
                    runtimeMode = runtimeMode,
                ),
            ),
            responseContractVersion = SWIFT_PRODUCT_CONTRACT_VERSION,
            generationMode = SWIFT_PRODUCT_GENERATION_MODE,
            swiftOutputDirectory = swiftOutputDirectory,
        )
    }

    private fun parseSwiftProductPackageRequest(
        request: JsonObject,
        requestFile: File,
    ): ParsedCompilerRequest {
        val contractVersion = request.optionalInt("contractVersion") ?: LEGACY_CONTRACT_VERSION
        require(contractVersion == SWIFT_PRODUCT_CONTRACT_VERSION) {
            "generationMode '$SWIFT_PRODUCT_PACKAGE_GENERATION_MODE' requires " +
                "contractVersion=$SWIFT_PRODUCT_CONTRACT_VERSION, but request specified $contractVersion."
        }

        rejectSwiftProductLegacyFields(request, SWIFT_PRODUCT_PACKAGE_GENERATION_MODE)

        val runtimeMode = SwiftProductRuntimeMode.fromId(request.requiredString("runtime"))
        val compilerOutputDirectory = File(request.requiredString("compilerOutputDirectory"))
        val databaseName = requireValidSwiftIdentifier(request.requiredString("databaseName"), "Swift package databaseName")
        val swiftPackageName = requireValidSwiftPackageName(request.requiredString("swiftPackageName"), "Swift package swiftPackageName")
        val swiftTargetName = requireValidSwiftIdentifier(request.requiredString("swiftTargetName"), "Swift package swiftTargetName")
        val swiftOutputDirectory = request.optionalString("swiftOutputDirectory")
            ?.let(::File)
            ?: defaultSwiftOutputDirectory(compilerOutputDirectory, swiftTargetName)
        val swiftPackageOutputDirectory = request.optionalString("swiftPackageOutputDirectory")
            ?.let(::File)
            ?: File(DEFAULT_SWIFT_PACKAGE_OUTPUT_ROOT).resolve(swiftPackageName)
        val runtimeModuleName = requireValidSwiftIdentifier(
            request.optionalString("runtimeModuleName") ?: runtimeMode.defaultRuntimeModuleName(),
            "Swift package runtimeModuleName",
        )
        val minimumPlatforms = request.optionalObject("minimumPlatforms")
            ?.let { platforms ->
                SwiftPackageMinimumPlatforms(
                    ios = platforms.optionalString("iOS") ?: SwiftPackageMinimumPlatforms().ios,
                    macos = platforms.optionalString("macOS") ?: SwiftPackageMinimumPlatforms().macos,
                )
            }
            ?: SwiftPackageMinimumPlatforms()
        val requestedAppleTargets = requireSupportedSwiftPackageAppleTargets(
            request.optionalStringArray("requestedAppleTargets") ?: DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS
        )
        val runtimeXcframeworkDirectory = request.optionalString("runtimeXcframeworkDirectory")?.let(::File)
        val runtimeArtifact = request.optionalObject("runtimeArtifact")?.let(::parseRuntimeArtifact)
        val sqliteNowPackage = request.optionalObject("sqliteNowPackage")?.let(::parseSqliteNowPackageDependency)
        val sqliteNowVersion = request.requiredString("sqliteNowVersion")
        val generatorVersion = generatorVersion()
        val resolvedRuntimeArtifact = resolveSwiftPackageRuntimeArtifact(
            runtimeXcframeworkDirectory = runtimeXcframeworkDirectory,
            runtimeArtifact = runtimeArtifact,
        )
        requireValidSwiftPackageRuntimeArtifact(
            runtimeArtifact = resolvedRuntimeArtifact,
            context = SwiftPackageRuntimeArtifactValidationContext(
                runtimeModuleName = runtimeModuleName,
                sqliteNowVersion = sqliteNowVersion,
                generatorVersion = generatorVersion,
            ),
        )
        val metadataBaseDirectory = File(".")
        val sourceDigestBaseDirectory = File(".")

        return ParsedCompilerRequest(
            input = SqliteNowCompilerInput(
                databaseName = databaseName,
                sqlDirectory = File(request.requiredString("sqlDirectory")),
                packageName = request.requiredString("metadataPackageName"),
                outputDirectory = compilerOutputDirectory,
                schemaDatabaseFile = request.optionalString("schemaDatabaseFile")?.let(::File),
                debug = request.optionalBoolean("debug") ?: false,
                backend = SqliteNowCompilerBackend.KOTLIN,
                swiftProductExport = SqliteNowSwiftProductExportConfig(
                    swiftOutputDirectory = swiftOutputDirectory,
                    swiftModuleName = swiftTargetName,
                    runtimeModuleName = runtimeModuleName,
                    runtimeMode = runtimeMode,
                    emitSupportSources = sqliteNowPackage == null,
                ),
            ),
            responseContractVersion = SWIFT_PRODUCT_CONTRACT_VERSION,
            generationMode = SWIFT_PRODUCT_PACKAGE_GENERATION_MODE,
            swiftOutputDirectory = swiftOutputDirectory,
            swiftPackage = ParsedSwiftProductPackageRequest(
                assemblerInput = SwiftPackageAssemblerInput(
                    databaseName = databaseName,
                    swiftPackageName = swiftPackageName,
                    swiftTargetName = swiftTargetName,
                    runtimeMode = runtimeMode,
                    runtimeModuleName = runtimeModuleName,
                    frameworkMode = requireSupportedSwiftPackageFrameworkMode(
                        request.optionalString("frameworkMode") ?: SWIFT_PACKAGE_FRAMEWORK_MODE_DYNAMIC,
                    ),
                    requestedAppleTargets = requestedAppleTargets,
                    minimumPlatforms = minimumPlatforms,
                    sqliteNowVersion = sqliteNowVersion,
                    generatorVersion = generatorVersion,
                    generatedBy = request.optionalString("generatedBy")
                        ?: "sqlitenow-compiler --request ${requestFile.invariantSeparatorsPath}",
                    generatedSwiftSourceDirectory = swiftOutputDirectory,
                    runtimeXcframeworkDirectory = runtimeXcframeworkDirectory,
                    runtimeArtifact = runtimeArtifact,
                    sqliteNowPackage = sqliteNowPackage,
                    sqlInputFiles = sortedSqlFiles(File(request.requiredString("sqlDirectory"))),
                    sourceDigestBaseDirectory = sourceDigestBaseDirectory,
                    packageRootDirectory = swiftPackageOutputDirectory,
                    metadataBaseDirectory = metadataBaseDirectory,
                    tools = SwiftPackageToolsMetadata(),
                    cleanPolicy = SwiftPackageCleanPolicy.REQUIRE_GENERATED_MARKER_OR_EMPTY,
                ),
            ),
        )
    }

    private fun rejectSwiftProductLegacyFields(request: JsonObject, generationMode: String) {
        val conflictingFields = listOf("backend", "packageName", "outputDirectory", "oversqlite")
            .filter { field -> request.containsKey(field) }
        require(conflictingFields.isEmpty()) {
            "generationMode '$generationMode' cannot be combined with legacy request field(s): " +
                conflictingFields.joinToString(", ")
        }
    }

    private fun parseRuntimeArtifact(runtimeArtifact: JsonObject): SwiftPackageRuntimeArtifact =
        parseSwiftPackageRuntimeArtifactRequest(
            kind = runtimeArtifact.requiredString("kind"),
            path = runtimeArtifact.optionalString("path"),
            url = runtimeArtifact.optionalString("url"),
            checksum = runtimeArtifact.optionalString("checksum"),
            sqliteNowVersion = runtimeArtifact.optionalString("sqliteNowVersion"),
        )

    private fun parseSqliteNowPackageDependency(sqliteNowPackage: JsonObject): SwiftPackageDependency =
        SwiftPackageDependency(
            kind = SwiftPackageDependencyKind.fromId(sqliteNowPackage.requiredString("kind")),
            packageIdentity = sqliteNowPackage.optionalString("packageIdentity") ?: "sqlitenow-kmp",
            path = sqliteNowPackage.optionalString("path"),
            url = sqliteNowPackage.optionalString("url"),
            version = sqliteNowPackage.optionalString("version"),
            coreRuntimeProduct = sqliteNowPackage.optionalString("coreRuntimeProduct") ?: DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME,
            syncRuntimeProduct = sqliteNowPackage.optionalString("syncRuntimeProduct") ?: DEFAULT_SWIFT_SYNC_RUNTIME_MODULE_NAME,
            coreSupportProduct = sqliteNowPackage.optionalString("coreSupportProduct") ?: "SQLiteNowCoreSupport",
            syncSupportProduct = sqliteNowPackage.optionalString("syncSupportProduct") ?: "SQLiteNowSyncSupport",
        )

    private fun successResponse(
        result: SqliteNowCompilerResult,
        javaMajorVersion: Int,
        request: ParsedCompilerRequest,
        packageResult: SwiftPackageAssemblerResult? = null,
    ): JsonObject = buildJsonObject {
        put("success", true)
        put("contractVersion", request.responseContractVersion)
        request.generationMode?.let { put("generationMode", it) }
        put("generatorVersion", generatorVersion())
        put("java", javaResponse(javaMajorVersion))
        val sourceGeneratedFilesAbsolute = result.generatedFiles.map { it.absoluteFile.normalize().invariantSeparatorsPath }
        val reportedGeneratedFiles = packageResult?.generatedFiles ?: result.generatedFiles
        val absoluteGeneratedFiles = reportedGeneratedFiles.map { it.absoluteFile.normalize().invariantSeparatorsPath }
        put(
            "generatedFiles",
            buildJsonArray {
                absoluteGeneratedFiles.forEach { add(JsonPrimitive(it)) }
            }
        )
        request.swiftOutputDirectory?.let { swiftOutputDirectory ->
            val swiftOutputRoot = swiftOutputDirectory.absoluteFile.normalize()
            put("swiftOutputDirectory", swiftOutputRoot.invariantSeparatorsPath)
            put(
                "generatedSwiftFiles",
                buildJsonArray {
                    result.generatedFiles.forEach { file ->
                        add(JsonPrimitive(file.absoluteFile.normalize().relativeTo(swiftOutputRoot).invariantSeparatorsPath))
                    }
                }
            )
            put(
                "generatedSwiftFilesAbsolute",
                buildJsonArray {
                    sourceGeneratedFilesAbsolute.forEach { add(JsonPrimitive(it)) }
                }
            )
        }
        packageResult?.let { swiftPackage ->
            put("swiftPackageOutputDirectory", swiftPackage.packageRootDirectory.absoluteFile.normalize().invariantSeparatorsPath)
            put("packageSwiftFile", swiftPackage.packageSwiftFile.absoluteFile.normalize().invariantSeparatorsPath)
            put(
                "packageMetadataManifestFile",
                swiftPackage.metadataManifestFile.absoluteFile.normalize().invariantSeparatorsPath
            )
            put(
                "packagedSwiftFiles",
                buildJsonArray {
                    swiftPackage.packagedSwiftFiles.forEach { add(JsonPrimitive(it)) }
                }
            )
            put(
                "packagedSwiftFilesAbsolute",
                buildJsonArray {
                    swiftPackage.packagedSwiftFilePaths.forEach {
                        add(JsonPrimitive(it.absoluteFile.normalize().invariantSeparatorsPath))
                    }
                }
            )
            put(
                "runtimeArtifactPaths",
                buildJsonArray {
                    swiftPackage.runtimeArtifactPaths.forEach { add(JsonPrimitive(it)) }
                }
            )
            put(
                "runtimeArtifactPathsAbsolute",
                buildJsonArray {
                    swiftPackage.runtimeArtifactAbsolutePaths.forEach {
                        add(JsonPrimitive(it.absoluteFile.normalize().invariantSeparatorsPath))
                    }
                }
            )
            put("runtimeArtifactKind", swiftPackage.runtimeArtifactKind.id)
            putNullableString("runtimeArtifactChecksum", swiftPackage.runtimeArtifactChecksum)
            putNullableString("runtimeArtifactVersion", swiftPackage.runtimeArtifactVersion)
            putNullableString("runtimeArtifactUrl", swiftPackage.runtimeArtifactUrl)
            put("sourceInputDigest", swiftPackage.sourceInputDigest)
        }
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
        contractVersion: Int,
    ): JsonObject = buildJsonObject {
        put("success", false)
        put("contractVersion", contractVersion)
        put("generatorVersion", generatorVersion())
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

    private fun defaultSwiftOutputDirectory(compilerOutputDirectory: File, swiftTargetName: String): File {
        val parent = compilerOutputDirectory.absoluteFile.parentFile ?: File(".").absoluteFile
        return parent.resolve("swift-product-source").resolve(swiftTargetName)
    }

    private fun sortedSqlFiles(sqlDirectory: File): List<File> =
        sqlDirectory.walkTopDown()
            .filter { it.isFile && it.extension == "sql" }
            .sortedBy { it.invariantSeparatorsPath }
            .toList()

    private fun contractResponse(javaMajorVersion: Int): JsonObject = buildJsonObject {
        put("success", true)
        put("contractVersion", LATEST_CONTRACT_VERSION)
        put(
            "supportedContractVersions",
            buildJsonArray {
                add(JsonPrimitive(LEGACY_CONTRACT_VERSION))
                add(JsonPrimitive(SWIFT_PRODUCT_CONTRACT_VERSION))
            }
        )
        put("generatorVersion", generatorVersion())
        put("java", javaResponse(javaMajorVersion))
        val legacyRequest = buildJsonObject {
            put("contractVersion", "optional-integer-1")
            put("databaseName", "string")
            put("sqlDirectory", "absolute-or-relative-path")
            put("packageName", "string")
            put("outputDirectory", "absolute-or-relative-path")
            put("schemaDatabaseFile", "optional-path-or-null")
            put("debug", "optional-boolean")
            put("oversqlite", "optional-boolean")
            put("backend", "optional-string-kotlin-or-dart")
        }
        val swiftProductSourceRequest = buildJsonObject {
            put("contractVersion", SWIFT_PRODUCT_CONTRACT_VERSION)
            put("generationMode", SWIFT_PRODUCT_GENERATION_MODE)
            put("databaseName", "string")
            put("sqlDirectory", "absolute-or-relative-path")
            put("metadataPackageName", "string")
            put("compilerOutputDirectory", "absolute-or-relative-path")
            put("swiftOutputDirectory", "absolute-or-relative-path")
            put("swiftModuleName", "string")
            put("runtime", "string-core-or-sync")
            put("runtimeModuleName", "optional-string-defaults-by-runtime")
            put("schemaDatabaseFile", "optional-path-or-null")
            put("debug", "optional-boolean")
        }
        val swiftProductPackageRequest = buildJsonObject {
            put("contractVersion", SWIFT_PRODUCT_CONTRACT_VERSION)
            put("generationMode", SWIFT_PRODUCT_PACKAGE_GENERATION_MODE)
            put("databaseName", "string")
            put("sqlDirectory", "absolute-or-relative-path")
            put("metadataPackageName", "string")
            put("compilerOutputDirectory", "absolute-or-relative-path")
            put("swiftOutputDirectory", "optional-absolute-or-relative-path-for-intermediate-source")
            put("swiftPackageOutputDirectory", "optional-path-defaults-to-$DEFAULT_SWIFT_PACKAGE_OUTPUT_ROOT/<swiftPackageName>")
            put("swiftPackageName", "string")
            put("swiftTargetName", "string")
            put("runtime", "string-core-or-sync")
            put("runtimeModuleName", "optional-string-defaults-by-runtime")
            put("runtimeXcframeworkDirectory", "optional-absolute-or-relative-path-mutually-exclusive-with-runtimeArtifact")
            put(
                "runtimeArtifact",
                buildJsonObject {
                    put("kind", "optional-object-kind-localZip-or-remoteZip")
                    put("path", "path-required-for-localZip")
                    put("checksum", "swiftpm-sha256-required-for-localZip-or-remoteZip")
                    put("sqliteNowVersion", "string-required-for-localZip-or-remoteZip")
                    put("url", "https-url-required-for-remoteZip")
                }
            )
            put("sqliteNowVersion", "string")
            put("frameworkMode", "optional-string-defaults-to-dynamic")
            put("requestedAppleTargets", "optional-string-array")
            put(
                "minimumPlatforms",
                buildJsonObject {
                    put("iOS", "optional-string-defaults-to-$DEFAULT_SWIFT_PACKAGE_MINIMUM_IOS")
                    put("macOS", "optional-string-defaults-to-$DEFAULT_SWIFT_PACKAGE_MINIMUM_MACOS")
                }
            )
            put("generatedBy", "optional-string")
            put("schemaDatabaseFile", "optional-path-or-null")
            put("debug", "optional-boolean")
        }
        put("request", legacyRequest)
        put(
            "requests",
            buildJsonObject {
                put("legacyKotlinOrDart", legacyRequest)
                put(SWIFT_PRODUCT_GENERATION_MODE, swiftProductSourceRequest)
                put(SWIFT_PRODUCT_PACKAGE_GENERATION_MODE, swiftProductPackageRequest)
            }
        )
        put("response", buildJsonObject {
            put("success", "boolean")
            put("contractVersion", "integer")
            put("generationMode", "optional-string")
            put("generatorVersion", "string")
            put("java", "object")
            put("generatedFiles", "absolute-path-string-array; package files for swiftProductPackage")
            put("swiftOutputDirectory", "absolute-path-string-only-for-swiftProductSource-or-swiftProductPackage")
            put("generatedSwiftFiles", "string-array-relative-to-swiftOutputDirectory-for-swiftProductSource-or-swiftProductPackage")
            put("generatedSwiftFilesAbsolute", "absolute-path-string-array-for-generated-swift-source")
            put("swiftPackageOutputDirectory", "absolute-path-string-only-for-swiftProductPackage")
            put("packageSwiftFile", "absolute-path-string-only-for-swiftProductPackage")
            put("packageMetadataManifestFile", "absolute-path-string-only-for-swiftProductPackage")
            put("packagedSwiftFiles", "string-array-relative-to-swiftPackageOutputDirectory-only-for-swiftProductPackage")
            put("packagedSwiftFilesAbsolute", "absolute-path-string-array-only-for-swiftProductPackage")
            put("runtimeArtifactPaths", "string-array-relative-to-swiftPackageOutputDirectory-only-for-swiftProductPackage")
            put("runtimeArtifactPathsAbsolute", "absolute-path-string-array-only-for-swiftProductPackage")
            put("runtimeArtifactKind", "string-only-for-swiftProductPackage")
            put("runtimeArtifactChecksum", "optional-string-only-for-swiftProductPackage")
            put("runtimeArtifactVersion", "optional-string-only-for-swiftProductPackage")
            put("runtimeArtifactUrl", "optional-string-only-for-swiftProductPackage")
            put("sourceInputDigest", "string-only-for-swiftProductPackage")
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

    private fun generatorVersion(): String =
        SqliteNowCompilerCli::class.java.`package`.implementationVersion
            ?.takeIf { it.isNotBlank() }
            ?: UNKNOWN_GENERATOR_VERSION

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

    private fun JsonObject.optionalInt(name: String): Int? =
        this[name]
            ?.takeUnless { it is JsonNull }
            ?.jsonPrimitive
            ?.content
            ?.toInt()

    private fun JsonObject.optionalStringArray(name: String): List<String>? =
        this[name]
            ?.takeUnless { it is JsonNull }
            ?.jsonArray
            ?.map { it.jsonPrimitive.content }
            ?.filter { it.isNotBlank() }

    private fun JsonObject.optionalObject(name: String): JsonObject? =
        this[name]
            ?.takeUnless { it is JsonNull }
            ?.jsonObject

    private fun JsonObject.optionalBackend(name: String): SqliteNowCompilerBackend? =
        optionalString(name)?.let { raw ->
            when (raw.lowercase()) {
                "kotlin" -> SqliteNowCompilerBackend.KOTLIN
                "dart" -> SqliteNowCompilerBackend.DART
                else -> error("Unsupported compiler backend '$raw'. Expected 'kotlin' or 'dart'.")
            }
        }
}
