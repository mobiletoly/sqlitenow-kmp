import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
    id(libs.plugins.androidApplication.get().pluginId)
    id(libs.plugins.jetbrainsCompose.get().pluginId)
    id(libs.plugins.composeCompiler.get().pluginId)
    id(libs.plugins.serialization.get().pluginId)
    id("dev.goquick.sqlitenow")
}

kotlin {
    androidTarget {
        compilations.all {
            this@androidTarget.compilerOptions {
                jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_11)
            }
        }
    }

    compilerOptions {
        languageVersion.set(KotlinVersion.KOTLIN_2_2)
        freeCompilerArgs.set(listOf("-Xmulti-dollar-interpolation"))
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

//    task("testClasses")

    sourceSets {
        commonMain.dependencies {
            implementation(compose.runtime)
            implementation(compose.foundation)
            implementation(compose.ui)
            implementation(compose.material)
            implementation(compose.components.resources)
            implementation(compose.components.uiToolingPreview)
            implementation(libs.sqlite.bundled)
            implementation(libs.kotlinx.datetime)
            implementation(libs.kotlinx.serialization.json)
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-cbor:1.8.1")
            implementation(libs.sqlite.bundled)

            implementation(project(":library"))
        }

        androidMain.dependencies {
            implementation(libs.androidx.activityCompose)
            implementation(libs.compose.ui.tooling)
            implementation(libs.compose.ui.toolingPreview)
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
        create("NowSampleDatabase") {
            packageName = "dev.goquick.sqlitenow.samplekmp.db"
            debug = true
        }
    }
}
