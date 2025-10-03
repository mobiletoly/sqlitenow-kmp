package dev.goquick.sqlitenow.gradle.util

/** Utility helpers for manipulating alias path chains (e.g., p->a->b). */
object AliasPathUtils {

    /** Lowercase every alias segment for case-insensitive comparisons. */
    fun lowercase(path: List<String>): List<String> = path.map { it.lowercase() }

    /** Case-insensitive prefix check for alias path lists. */
    fun startsWith(path: List<String>, prefix: List<String>): Boolean {
        if (prefix.isEmpty() || path.size < prefix.size) return false
        prefix.indices.forEach { idx ->
            if (!path[idx].equals(prefix[idx], ignoreCase = true)) return false
        }
        return true
    }
}
