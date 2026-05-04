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
package dev.goquick.sqlitenow.gradle.util

import com.squareup.kotlinpoet.FileSpec
import dev.goquick.sqlitenow.gradle.logger
import java.io.File

/**
 * Helper class for common file generation patterns.
 * Provides shared functionality for generating and writing Kotlin code files.
 */
class FileGenerationHelper(
    val packageName: String,
    private val outputDir: File
) {

    /**
     * Generates code files for a set of namespaces using a provided file generator function.
     * This centralizes the common pattern of:
     * 1. Creating output directory
     * 2. Iterating through namespaces
     * 3. Generating FileSpec for each namespace
     * 4. Writing files to output directory
     *
     * @param namespaces Set of namespace names to generate files for
     * @param fileGenerator Function that takes (namespace, packageName) and returns FileSpec.Builder
     */
    fun generateFiles(
        namespaces: Set<String>,
        fileGenerator: (namespace: String, packageName: String) -> FileSpec.Builder
    ) {
        if (!outputDir.exists()) {
            outputDir.mkdirs()
        }

        namespaces.forEach { namespace ->
            try {
                val fileSpecBuilder = fileGenerator(namespace, packageName)
                fileSpecBuilder.build().writeTo(outputDir)
            } catch (e: Exception) {
                logger.error("Failed to generate code for namespace '$namespace'")
                logger.error("Package: $packageName")
                logger.error("Output directory: ${outputDir.absolutePath}")
                throw RuntimeException("Code generation failed for namespace '$namespace'", e)
            }
        }
    }
}
