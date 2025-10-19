import org.jetbrains.kotlin.gradle.dsl.KotlinVersion
import org.gradle.language.jvm.tasks.ProcessResources
import org.jetbrains.kotlin.gradle.targets.js.dsl.ExperimentalWasmDsl

plugins {
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
    id(libs.plugins.androidApplication.get().pluginId)
    id(libs.plugins.jetbrainsCompose.get().pluginId)
    id(libs.plugins.composeCompiler.get().pluginId)
    id(libs.plugins.serialization.get().pluginId)
    id("dev.goquick.sqlitenow")
}

kotlin {
    applyDefaultHierarchyTemplate()

    androidTarget {
        compilations.all {
            this@androidTarget.compilerOptions {
                jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_11)
            }
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

    @OptIn(ExperimentalWasmDsl::class)
    wasmJs {
        browser()
        binaries.executable()
    }

//    task("testClasses")

    sourceSets {
        commonMain.dependencies {
            implementation(compose.runtime)
            implementation(compose.foundation)
            implementation(compose.ui)
            implementation(compose.material)
            implementation(compose.materialIconsExtended)
            implementation(compose.components.resources)
            implementation(compose.components.uiToolingPreview)
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
            implementation(compose.runtime)
            implementation(compose.foundation)
            implementation(compose.material)
            implementation(compose.ui)
            implementation(devNpm("copy-webpack-plugin", "11.0.0"))
            implementation(npm("sql.js", "1.13.0"))
        }

        wasmJsMain.dependencies {
            implementation(compose.runtime)
            implementation(compose.foundation)
            implementation(compose.material)
            implementation(compose.ui)
            implementation(devNpm("copy-webpack-plugin", "11.0.0"))
            implementation(npm("sql.js", "1.13.0"))
        }
    }
}

android {
    compileSdk = libs.versions.android.compileSdk.get().toInt()
    namespace = "dev.goquick.sqlitenow.samplesynckmp"

    sourceSets["main"].manifest.srcFile("src/androidMain/AndroidManifest.xml")
    sourceSets["main"].res.srcDirs("src/androidMain/res")

    defaultConfig {
        applicationId = "dev.goquick.sqlitenow.samplesynckmp"
        minSdk = libs.versions.android.minSdk.get().toInt()
        targetSdk = libs.versions.android.targetSdk.get().toInt()
        versionCode = 1
        versionName = "1.0"
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    packaging {
        resources {
            excludes += "/META-INF/{AL2.0,LGPL2.1}"
        }
    }

    buildFeatures {
        compose = true
        buildConfig = true
    }

    lint {
        abortOnError = false
    }

    dependencies {
    }
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_11)
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
