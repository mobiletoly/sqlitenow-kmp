package dev.goquick.sqlitenow.oversqlite

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.MessageDigest
import kotlin.io.path.isRegularFile
import kotlin.io.path.readLines

internal fun oversqliteContractFixture(relativePath: String): Path {
    val corpusRoot = configuredOversqliteContractRoot(System.getenv("OVERSQLITE_CONTRACTS_ROOT"))
    validateManifestPinnedCorpus(corpusRoot)
    return corpusRoot.resolve(relativePath)
}

internal fun validateManifestPinnedCorpus(corpusRoot: Path) {
    val manifest = corpusRoot.resolve("MANIFEST.sha256")
    check(manifest.isRegularFile()) { "missing shared Oversqlite corpus manifest: $manifest" }
    val lines = manifest.readLines()
    val paths = lines.map { line ->
        val parts = line.split("  ", limit = 2)
        check(parts.size == 2 && parts[0].matches(Regex("[0-9a-f]{64}")) && parts[1].isNotBlank()) {
            "invalid shared Oversqlite corpus manifest record: $line"
        }
        val normalized = Path.of(parts[1]).normalize().toString().replace('\\', '/')
        check(parts[1].replace('\\', '/') == parts[1] && !Path.of(parts[1]).isAbsolute &&
            normalized == parts[1] && parts[1] != "." && !parts[1].startsWith("../")) {
            "manifest path must be relative POSIX: ${parts[1]}"
        }
        parts[1]
    }
    check(paths == paths.sorted() && paths.distinct().size == paths.size) {
        "shared Oversqlite corpus manifest paths must be strictly sorted and unique"
    }
    val expected = lines.associate { line ->
        val parts = line.split("  ", limit = 2)
        parts[1] to parts[0]
    }
    check(expected.isNotEmpty()) { "shared Oversqlite corpus manifest is empty" }
    val actual = Files.walk(corpusRoot).use { paths ->
        paths.filter { it.isRegularFile() && it != manifest }
            .map { corpusRoot.relativize(it).toString().replace('\\', '/') }
            .sorted()
            .toList()
    }
    check(actual.size == expected.size) { "shared Oversqlite corpus manifest file count mismatch" }
    actual.forEach { relativePath ->
        val expectedHash = expected[relativePath]
            ?: error("shared Oversqlite corpus manifest omits $relativePath")
        val bytes = Files.readAllBytes(corpusRoot.resolve(relativePath))
        val actualHash = MessageDigest.getInstance("SHA-256").digest(bytes)
            .joinToString("") { "%02x".format(it) }
        check(actualHash == expectedHash) {
            "shared Oversqlite corpus manifest hash mismatch for $relativePath"
        }
    }
}

internal fun configuredOversqliteContractRoot(configured: String?): Path {
    check(!configured.isNullOrBlank()) { "OVERSQLITE_CONTRACTS_ROOT is required for shared contract tests" }
    return Paths.get(configured)
}
