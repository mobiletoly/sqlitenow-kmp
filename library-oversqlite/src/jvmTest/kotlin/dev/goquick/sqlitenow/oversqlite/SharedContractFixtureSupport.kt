package dev.goquick.sqlitenow.oversqlite

import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.exists

internal fun oversqliteContractFixture(relativePath: String): Path {
    return findRepoRoot().resolve("oversqlite-contracts").resolve(relativePath)
}

private fun findRepoRoot(): Path {
    var current = Paths.get("").toAbsolutePath()
    while (true) {
        if (current.resolve("settings.gradle.kts").exists()) {
            return current
        }
        current = current.parent
            ?: error("could not locate repository root from ${Paths.get("").toAbsolutePath()}")
    }
}
