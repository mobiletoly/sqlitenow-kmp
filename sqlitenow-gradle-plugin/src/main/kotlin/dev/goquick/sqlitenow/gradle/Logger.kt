package dev.goquick.sqlitenow.gradle

import org.gradle.api.logging.Logging

internal val logger by lazy {
    Logging.getLogger("sqlitenow")
}