import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
    id(libs.plugins.androidKotlinMultiplatformLibrary.get().pluginId)
    id(libs.plugins.jetbrainsCompose.get().pluginId)
    id(libs.plugins.composeCompiler.get().pluginId)
    id(libs.plugins.serialization.get().pluginId)
    id("dev.goquick.sqlitenow")
}

kotlin {
    jvmToolchain(17)
    androidLibrary {
        namespace = "dev.goquick.sqlitenow.daytempokmp"
        compileSdk = libs.versions.android.compileSdk.get().toInt()
        minSdk = libs.versions.android.minSdk.get().toInt()

        androidResources {
            enable = true
        }

        withDeviceTestBuilder {
            sourceSetTreeName = "test"
        }

        compilerOptions {
            jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
        }
    }

    compilerOptions {
        languageVersion.set(KotlinVersion.KOTLIN_2_2)
        freeCompilerArgs.addAll("-Xmulti-dollar-interpolation")
    }

    sourceSets {
        commonMain.dependencies {
            implementation(libs.jetbrains.compose.runtime)
            implementation(libs.jetbrains.compose.foundation)
            implementation(libs.jetbrains.compose.ui)
            implementation(libs.jetbrains.compose.material)
            implementation(libs.jetbrains.compose.components.resources)
            implementation(libs.jetbrains.compose.components.uiToolingPreview)
            implementation(libs.sqlite.bundled)
            implementation(libs.kotlinx.datetime)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.serialization.cbor)
            implementation(libs.sqlite.bundled)
            implementation(libs.kermit)

            implementation(project(":library"))
        }

        androidMain.dependencies {
            implementation(libs.androidx.activityCompose)
            implementation(libs.compose.ui.tooling)
            implementation(libs.compose.ui.toolingPreview)
        }

        // Android device tests (device/emulator). Keep it minimal; used to compile generated code.
        getByName("androidDeviceTest").dependencies {
            implementation(libs.androidx.junit)
            implementation(libs.androidx.test.runner)
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
        create("DayTempoDatabase") {
            packageName = "dev.goquick.sqlitenow.daytempokmp.db"
            debug = false
        }
    }
}
