package dev.goquick.sqlitenow.gradle.util

/**
 * Simple map wrapper that stores lowercase keys for case-insensitive lookups.
 */
internal class CaseInsensitiveMap<V>(
    entries: Iterable<Pair<String, V>>,
) {
    private val delegate: Map<String, V> = entries.associate { it.first.lowercase() to it.second }

    operator fun get(key: String): V? = delegate[key.lowercase()]

    fun containsKey(key: String): Boolean = delegate.containsKey(key.lowercase())
}
