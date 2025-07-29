package dev.goquick.sqlitenow.gradle

sealed interface AnnotatedStatement {
    val name: String
    val annotations: StatementAnnotationOverrides

    fun getDataClassName(): String {
        return annotations.name ?: name.capitalized()
    }
}
