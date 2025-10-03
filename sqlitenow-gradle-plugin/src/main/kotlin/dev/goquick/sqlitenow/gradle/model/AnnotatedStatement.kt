package dev.goquick.sqlitenow.gradle.model

import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.util.capitalized

sealed interface AnnotatedStatement {
    val name: String
    val annotations: StatementAnnotationOverrides

    fun getDataClassName(): String {
        return annotations.name ?: name.capitalized()
    }
}
