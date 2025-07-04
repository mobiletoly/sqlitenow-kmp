package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.FileSpec
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
            val fileSpecBuilder = fileGenerator(namespace, packageName)
            fileSpecBuilder.build().writeTo(outputDir)
        }
    }
}
