package dev.goquick.sqlitenow.gradle.util

internal fun String.capitalized(): String {
    return this.replaceFirstChar { it.titlecase() }
}

internal fun pascalize(source: String): String = source
    .split('_', '-', ' ')
    .filter { it.isNotBlank() }
    .joinToString("") { it.replaceFirstChar { c -> c.uppercase() } }

internal fun String.lowercaseFirst(): String {
    return this.replaceFirstChar { it.lowercase() }
}
