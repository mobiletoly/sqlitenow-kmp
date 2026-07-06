import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
}

kotlin {
    jvmToolchain(17)
    applyDefaultHierarchyTemplate()

    compilerOptions {
        languageVersion.set(KotlinVersion.KOTLIN_2_4)
        freeCompilerArgs.add("-Xexpect-actual-classes")
    }

    listOf(
        macosArm64(),
        iosArm64(),
        iosSimulatorArm64(),
    ).forEach { target ->
        target.binaries.framework {
            baseName = "SQLiteNowSyncRuntime"
            isStatic = false
            export(project(":swift:runtime:core"))
        }
    }

    sourceSets {
        commonMain.dependencies {
            implementation(project(":library-oversqlite"))
            implementation(libs.ktor.client.core)
            implementation(libs.ktor.client.auth)
            implementation(libs.ktor.client.content.negotiation)
            implementation(libs.ktor.serialization.kotlinx.json)
        }

        val appleMain by getting {
            dependencies {
                api(project(":swift:runtime:core"))
                implementation(libs.ktor.client.darwin)
            }
        }
    }
}

extra["swiftRuntimeMode"] = "sync"
extra["swiftRuntimeFrameworkModuleName"] = "SQLiteNowSyncRuntime"
apply(from = rootProject.file("gradle/swift-runtime-artifact-packaging.gradle.kts"))
