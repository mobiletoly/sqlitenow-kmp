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

import dev.goquick.sqlitenow.gradle.generator.data.DataStructCodeGenerator
import java.io.File

data class SqliteNowSwiftExportConfig(
    val swiftOutputDirectory: File,
    val swiftModuleName: String,
    val frameworkModuleName: String,
    val bridgePackageName: String,
)

internal class SwiftCodeGenerator(
    private val databaseName: String,
    private val databasePackageName: String,
    private val kotlinOutputDir: File,
    private val dataStructCodeGenerator: DataStructCodeGenerator,
    private val config: SqliteNowSwiftExportConfig,
    private val oversqlite: Boolean = false,
) {
    private val context = dataStructCodeGenerator.generatorContext
    private val plan = SwiftGenerationPlan(context, dataStructCodeGenerator)
    private val model = SwiftLegacyExportModel(context, plan)
    private val swiftOversqliteEnabled: Boolean
        get() = oversqlite && context.createTableStatements.any { it.annotations.enableSync }

    fun generateCode(): List<File> {
        val bridgeFile = writeKotlinBridge()
        val swiftFile = writeSwiftOverlay()
        return listOf(bridgeFile, swiftFile)
    }

    private fun writeKotlinBridge(): File {
        val outputFile = kotlinOutputDir
            .resolve(config.bridgePackageName.replace('.', File.separatorChar))
            .resolve("${databaseName}Bridge.kt")
        outputFile.parentFile.mkdirs()
        outputFile.writeText(
            SwiftKotlinBridgeEmitter(
                databaseName = databaseName,
                databasePackageName = databasePackageName,
                config = config,
                oversqlite = oversqlite,
                context = context,
                plan = plan,
                model = model,
            ).emit()
        )
        return outputFile
    }

    private fun writeSwiftOverlay(): File {
        config.swiftOutputDirectory.mkdirs()
        val outputFile = config.swiftOutputDirectory.resolve("$databaseName.swift")
        outputFile.writeText(
            SwiftOverlayEmitter(
                databaseName = databaseName,
                config = config,
                context = context,
                model = model,
                swiftOversqliteEnabled = swiftOversqliteEnabled,
            ).emit()
        )
        return outputFile
    }
}
