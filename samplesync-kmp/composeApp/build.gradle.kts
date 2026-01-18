import org.jetbrains.kotlin.gradle.dsl.KotlinVersion
import org.gradle.language.jvm.tasks.ProcessResources

plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.androidKotlinMultiplatformLibrary)
    alias(libs.plugins.jetbrainsCompose)
    alias(libs.plugins.composeCompiler)
    alias(libs.plugins.serialization)
    id("dev.goquick.sqlitenow")
}

kotlin {
    jvmToolchain(17)
    applyDefaultHierarchyTemplate()

    androidLibrary {
        namespace = "dev.goquick.sqlitenow.samplesynckmp"
        compileSdk = libs.versions.android.compileSdk.get().toInt()
        minSdk = libs.versions.android.minSdk.get().toInt()

        androidResources {
            enable = true
        }

        compilerOptions {
            jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
        }
    }

    compilerOptions {
        languageVersion.set(KotlinVersion.KOTLIN_2_2)
        freeCompilerArgs.addAll("-Xmulti-dollar-interpolation")
    }

    listOf(
        iosX64(),
        iosArm64(),
        iosSimulatorArm64()
    ).forEach {
        it.binaries.framework {
            baseName = "ComposeApp"
            isStatic = true
        }
    }

    js(IR) {
        browser {
            binaries.executable()
        }
    }

    @OptIn(org.jetbrains.kotlin.gradle.ExperimentalWasmDsl::class)
    wasmJs {
        browser()
        binaries.executable()
    }

//    task("testClasses")

    sourceSets {
        commonMain.dependencies {
            implementation(libs.jetbrains.compose.runtime)
            implementation(libs.jetbrains.compose.foundation)
            implementation(libs.jetbrains.compose.ui)
            implementation(libs.jetbrains.compose.material)
            implementation(libs.jetbrains.compose.material.iconsExtended)
            implementation(libs.jetbrains.compose.components.resources)
            implementation(libs.jetbrains.compose.components.uiToolingPreview)
            implementation(libs.kotlinx.datetime)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.serialization.cbor)

            implementation(project(":library"))
            implementation(libs.ktor.client.core)
            implementation(libs.ktor.client.auth)
            implementation(libs.ktor.client.content.negotiation)
            implementation(libs.ktor.serialization.kotlinx.json)
            // Multiplatform Settings
            implementation(libs.multiplatform.settings)
            implementation(libs.multiplatform.settings.no.arg)
            // Logging
            implementation(libs.kermit)
        }

        androidMain.dependencies {
            implementation(libs.androidx.activityCompose)
            implementation(libs.compose.ui.tooling)
            implementation(libs.compose.ui.toolingPreview)
            implementation(libs.ktor.client.okhttp)
        }

        iosMain.dependencies {
            implementation(libs.ktor.client.darwin)
        }

        jsMain.dependencies {
            implementation(libs.jetbrains.compose.runtime)
            implementation(libs.jetbrains.compose.foundation)
            implementation(libs.jetbrains.compose.material)
            implementation(libs.jetbrains.compose.ui)
            implementation(devNpm("copy-webpack-plugin", "11.0.0"))
            implementation(npm("sql.js", "1.13.0"))
        }

        wasmJsMain.dependencies {
            implementation(libs.jetbrains.compose.runtime)
            implementation(libs.jetbrains.compose.foundation)
            implementation(libs.jetbrains.compose.material)
            implementation(libs.jetbrains.compose.ui)
            implementation(devNpm("copy-webpack-plugin", "11.0.0"))
            implementation(npm("sql.js", "1.13.0"))
        }
    }
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
    }
}

sqliteNow {
    databases {
        create("NowSampleSyncDatabase") {
            packageName = "dev.goquick.sqlitenow.samplesynckmp.db"
        }
    }
}

val libraryProject = project(":library")
val librarySqlJsResource = libraryProject.layout.buildDirectory.file("processedResources/wasmJs/main/sqlitenow-sqljs.js")
val librarySqlWasmBinary = libraryProject.layout.buildDirectory.file("processedResources/wasmJs/main/sql-wasm.wasm")
val libraryIndexedDbResource = libraryProject.layout.buildDirectory.file("processedResources/wasmJs/main/sqlitenow-indexeddb.js")

tasks.named<ProcessResources>("wasmJsProcessResources") {
    dependsOn(libraryProject.tasks.named("wasmJsProcessResources"))
    from(librarySqlJsResource)
    from(librarySqlWasmBinary)
    from(libraryIndexedDbResource)
}
