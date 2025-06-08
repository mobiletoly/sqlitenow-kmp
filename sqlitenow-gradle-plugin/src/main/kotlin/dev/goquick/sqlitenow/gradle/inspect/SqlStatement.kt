package dev.goquick.sqlitenow.gradle.inspect

interface SqlStatement {
    val sql: String
    val namedParameters: List<String>
}
