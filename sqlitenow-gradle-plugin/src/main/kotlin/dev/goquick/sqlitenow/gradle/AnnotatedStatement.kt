package dev.goquick.sqlitenow.gradle

import org.gradle.internal.extensions.stdlib.capitalized

sealed interface AnnotatedStatement {
    val name: String
    val annotations: StatementAnnotationOverrides

    fun getDataClassName(): String {
        return annotations.name ?: name.capitalized()
    }
}
