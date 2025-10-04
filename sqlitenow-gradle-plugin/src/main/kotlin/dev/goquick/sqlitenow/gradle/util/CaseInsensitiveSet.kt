package dev.goquick.sqlitenow.gradle.util

/**
 * Simple helper for case-insensitive lookups while preserving the original casing of inserted values.
 */
internal class CaseInsensitiveSet {
    private val delegate = linkedSetOf<String>()
    private val lowercased = linkedSetOf<String>()

    fun add(value: String) {
        delegate += value
        lowercased += value.lowercase()
    }

    fun addAll(values: Iterable<String>) {
        values.forEach { add(it) }
    }

    fun containsIgnoreCase(value: String): Boolean = lowercased.contains(value.lowercase())
}
