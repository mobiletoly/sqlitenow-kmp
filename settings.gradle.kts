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
include(":library-oversqlite-test:composeApp")
include(":daytempo-kmp:composeApp")
include(":daytempo-kmp:androidApp")
include(":sample-kmp:composeApp")
include(":sample-kmp:androidApp")
include(":samplesync-kmp:composeApp")
include(":samplesync-kmp:androidApp")
