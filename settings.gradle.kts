pluginManagement {
    repositories {
        google {
            content {
                content {
                    includeGroupByRegex("com\\.android.*")
                    includeGroupByRegex("com\\.google.*")
                    includeGroupByRegex("androidx.*")
                }
            }
        }
        mavenCentral()
        gradlePluginPortal()
    }
    includeBuild("sqlitenow-gradle-plugin")
    plugins {
        kotlin("jvm") version "2.2.0"
    }
}

dependencyResolutionManagement {
    repositories {
        google {
            content {
                content {
                    includeGroupByRegex("com\\.android.*")
                    includeGroupByRegex("com\\.google.*")
                    includeGroupByRegex("androidx.*")
                }
            }
        }
        mavenCentral()
    }
}

rootProject.name = "sqlitenow-kmp"
include(":library")
include(":sample-kmp:composeApp")
