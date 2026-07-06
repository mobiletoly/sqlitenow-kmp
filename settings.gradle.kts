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
    includeBuild("sqlitenow-compiler")
}
plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

includeBuild("sqlitenow-compiler")

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
include(":library-core")
include(":library-oversqlite")
include(":platform-core-test:harness")
include(":platform-oversqlite-test:composeApp")
include(":sample-kmp:composeApp")
include(":sample-kmp:androidApp")
include(":samplesync-kmp:composeApp")
include(":samplesync-kmp:androidApp")
include(":swift:fixtures:package:core")
include(":swift:fixtures:package:sync")
include(":swift:runtime:core")
include(":swift:runtime:sync")
