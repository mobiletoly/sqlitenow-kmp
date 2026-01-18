import org.gradle.api.tasks.JavaExec
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion
import org.gradle.language.jvm.tasks.ProcessResources
import org.jetbrains.kotlin.gradle.targets.jvm.KotlinJvmTarget

val composeDesktopCurrentOsDependency = run {
    val os = System.getProperty("os.name").lowercase()
    val arch = System.getProperty("os.arch").lowercase()
    val targetId = when {
        os.contains("mac") || os.contains("darwin") ->
            if (arch.contains("aarch64") || arch.contains("arm64")) "macos-arm64" else "macos-x64"
        os.contains("win") ->
            if (arch.contains("aarch64") || arch.contains("arm64")) "windows-arm64" else "windows-x64"
        else ->
            if (arch.contains("aarch64") || arch.contains("arm64")) "linux-arm64" else "linux-x64"
    }
    "org.jetbrains.compose.desktop:desktop-jvm-$targetId:${libs.versions.compose.plugin.get()}"
}

plugins {
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
    id(libs.plugins.androidKotlinMultiplatformLibrary.get().pluginId)
    id(libs.plugins.jetbrainsCompose.get().pluginId)
//    id(libs.plugins.composeCompiler.get().pluginId)
    id(libs.plugins.serialization.get().pluginId)
    id("dev.goquick.sqlitenow")
    alias(libs.plugins.composeCompiler)
}

kotlin {
    jvmToolchain(17)
    applyDefaultHierarchyTemplate()

    androidLibrary {
        namespace = "dev.goquick.sqlitenow.samplekmp"
        compileSdk = libs.versions.android.compileSdk.get().toInt()
        minSdk = libs.versions.android.minSdk.get().toInt()

        androidResources {
            enable = true
        }
    }

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
            implementation(libs.jetbrains.compose.components.resources)
            implementation(libs.jetbrains.compose.components.uiToolingPreview)
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
            implementation(composeDesktopCurrentOsDependency)
        }

        jsMain.dependencies {
            implementation(libs.jetbrains.compose.runtime)
            implementation(libs.jetbrains.compose.foundation)
            implementation(libs.jetbrains.compose.material)
            implementation(libs.jetbrains.compose.ui)
            implementation(npm("sql.js", "1.13.0"))
        }

        wasmJsMain.dependencies {
            implementation(libs.jetbrains.compose.runtime)
            implementation(libs.jetbrains.compose.foundation)
            implementation(libs.jetbrains.compose.material)
            implementation(libs.jetbrains.compose.ui)
            implementation(npm("sql.js", "1.13.0"))
        }
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
