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
}
plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
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
include(":library-core-test:composeApp")
include(":platform-oversqlite-test:composeApp")
include(":sample-kmp:composeApp")
include(":sample-kmp:androidApp")
include(":samplesync-kmp:composeApp")
include(":samplesync-kmp:androidApp")
