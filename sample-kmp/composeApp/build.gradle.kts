import org.gradle.api.tasks.JavaExec
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion
import org.gradle.language.jvm.tasks.ProcessResources
import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.jetbrains.kotlin.gradle.targets.jvm.KotlinJvmTarget

plugins {
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
    id(libs.plugins.androidApplication.get().pluginId)
    id(libs.plugins.jetbrainsCompose.get().pluginId)
    id(libs.plugins.composeCompiler.get().pluginId)
    id(libs.plugins.serialization.get().pluginId)
    id("dev.goquick.sqlitenow")
}

kotlin {
    jvmToolchain(17)
    applyDefaultHierarchyTemplate()

    androidTarget()

    jvm("desktop")

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
            implementation(compose.components.resources)
            implementation(compose.components.uiToolingPreview)
            implementation(libs.kotlinx.datetime)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.serialization.cbor)
            implementation(libs.kermit)

            implementation(project(":library"))
        }

        androidMain.dependencies {
            implementation(libs.androidx.activityCompose)
            implementation(libs.compose.ui.tooling)
            implementation(libs.compose.ui.toolingPreview)
        }

        getByName("desktopMain").dependencies {
            implementation(compose.desktop.currentOs)
        }

        jsMain.dependencies {
            implementation(compose.runtime)
            implementation(compose.foundation)
            implementation(compose.material)
            implementation(compose.ui)
            implementation(npm("sql.js", "1.13.0"))
        }

        wasmJsMain.dependencies {
            implementation(compose.runtime)
            implementation(compose.foundation)
            implementation(compose.material)
            implementation(compose.ui)
            implementation(npm("sql.js", "1.13.0"))
        }
    }
}

android {
    compileSdk = libs.versions.android.compileSdk.get().toInt()
    namespace = "dev.goquick.sqlitenow.samplekmp"

    sourceSets["main"].manifest.srcFile("src/androidMain/AndroidManifest.xml")
    sourceSets["main"].res.srcDirs("src/androidMain/res")

    defaultConfig {
        applicationId = "dev.goquick.sqlitenow.samplekmp"
        minSdk = libs.versions.android.minSdk.get().toInt()
        targetSdk = libs.versions.android.targetSdk.get().toInt()
        versionCode = 1
        versionName = "1.0"
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
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
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
    }
}

compose.desktop {
    application {
        mainClass = "dev.goquick.sqlitenow.samplekmp.DesktopMainKt"
        buildTypes.release.proguard.isEnabled = false
    }
}

val desktopTarget = kotlin.targets.getByName("desktop") as KotlinJvmTarget
val desktopMainCompilation = desktopTarget.compilations.getByName("main")

tasks.register<JavaExec>("runDesktop") {
    group = "application"
    description = "Runs the sample app on the desktop JVM target."
    mainClass.set("dev.goquick.sqlitenow.samplekmp.DesktopMainKt")
    classpath = desktopMainCompilation.runtimeDependencyFiles
    classpath(desktopMainCompilation.output.allOutputs)
    dependsOn(desktopMainCompilation.compileTaskProvider)
}

sqliteNow {
    databases {
        create("NowSampleDatabase") {
            packageName = "dev.goquick.sqlitenow.samplekmp.db"
            debug = false
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
