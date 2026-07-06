package dev.goquick.sqlitenow.gradle

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import org.junit.jupiter.api.io.TempDir

class SwiftPackageLeakCheckerTest {
    @TempDir
    lateinit var tempDir: File

    @Test
    fun reportsLiteralAndRegexLeaksWithRelativePaths() {
        val sourceDir = tempDir.resolve("Sources/App").apply { mkdirs() }
        val sourceFile = sourceDir.resolve("Database.swift").apply {
            writeText(
                """
                import Foundation

                final class Database {
                    let path = "/repo/sqlitenow-kmp"
                    let token: KotlinByteArray? = nil
                }
                """.trimIndent()
            )
        }

        val leaks = SwiftPackageLeakChecker.findLeaks(
            files = listOf(sourceFile),
            rootDirectory = tempDir,
            forbiddenLiteralValues = listOf("/repo/sqlitenow-kmp"),
            forbiddenRegexPatterns = listOf("Kotlin\\w+"),
        )

        assertEquals(
            listOf(
                SwiftPackageLeak("Sources/App/Database.swift", "/repo/sqlitenow-kmp"),
                SwiftPackageLeak("Sources/App/Database.swift", "KotlinByteArray"),
            ),
            leaks,
        )
    }

    @Test
    fun defaultPatternsAllowFlowInNamesButRejectGenericFlowReferences() {
        val sourceDir = tempDir.resolve("Sources/App").apply { mkdirs() }
        val sourceFile = sourceDir.resolve("Database.swift").apply {
            writeText(
                """
                public struct DataFlowRow {}

                let stream: Flow<Item>? = nil
                """.trimIndent()
            )
        }

        val leaks = SwiftPackageLeakChecker.findLeaks(
            files = listOf(sourceFile),
            rootDirectory = tempDir,
            forbiddenRegexPatterns = SwiftPackageLeakChecker.DEFAULT_FORBIDDEN_REGEX_PATTERNS,
        )

        assertEquals(
            listOf(SwiftPackageLeak("Sources/App/Database.swift", "Flow<")),
            leaks,
        )
    }

    @Test
    fun ignoresBlankRulesDirectoriesAndMissingFiles() {
        val sourceDir = tempDir.resolve("Sources").apply { mkdirs() }
        val sourceFile = sourceDir.resolve("Database.swift").apply {
            writeText("let value = \"clean\"\n")
        }

        val leaks = SwiftPackageLeakChecker.findLeaks(
            files = listOf(sourceDir, sourceFile, tempDir.resolve("Missing.swift")),
            rootDirectory = tempDir,
            forbiddenLiteralValues = listOf("", " "),
            forbiddenRegexPatterns = listOf("", " "),
        )

        assertEquals(emptyList(), leaks)
    }
}
