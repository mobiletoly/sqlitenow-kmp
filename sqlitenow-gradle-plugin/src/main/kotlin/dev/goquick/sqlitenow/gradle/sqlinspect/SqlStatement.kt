package dev.goquick.sqlitenow.gradle.sqlinspect

interface SqlStatement {
    val sql: String
    val namedParameters: List<String>
}
