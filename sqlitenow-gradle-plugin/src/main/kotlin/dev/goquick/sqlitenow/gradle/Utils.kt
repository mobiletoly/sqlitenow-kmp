package dev.goquick.sqlitenow.gradle

import org.gradle.api.logging.Logging

internal val logger by lazy {
    Logging.getLogger("sqlitenow")
}

internal fun String.capitalized(): String {
    return this.replaceFirstChar { it.titlecase() }
}
