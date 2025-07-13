import org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id(libs.plugins.androidLibrary.get().pluginId)
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
    id(libs.plugins.mavenPublish.get().pluginId)
}

group = "dev.goquick.sqlitenow"
version = "0.1.15"

kotlin {
    jvm()
    androidTarget {
        publishLibraryVariants("release")
        @OptIn(ExperimentalKotlinGradlePluginApi::class)
        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_11)
        }
    }
    iosX64()
    iosArm64()
    iosSimulatorArm64()

    sourceSets {
        val commonMain by getting {
            dependencies {
                implementation(libs.sqlite.bundled)
                implementation(libs.kotlinx.coroutines.core)
                implementation(libs.kotlinx.datetime)
                implementation(libs.kotlinx.serialization.json)
            }
        }
        val commonTest by getting {
            dependencies {
                implementation(libs.kotlin.test)
                implementation(libs.kotlinx.coroutines.test)
            }
        }

        val jvmMain by getting {
            dependencies {
                // JVM-specific dependencies
            }
        }

        val androidMain by getting {
            dependencies {
            }
        }
    }
}

android {
    namespace = group.toString()
    compileSdk = libs.versions.android.compileSdk.get().toInt()
    defaultConfig {
        minSdk = libs.versions.android.minSdk.get().toInt()
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
}

mavenPublishing {
    publishToMavenCentral()

    signAllPublications()

    coordinates(group.toString(), "core", version.toString())

    pom {
        name = "SQLiteNow Multiplatform Library"
        description = "SQLiteNow Multiplatform Library"
        inceptionYear = "2025"
        url = "https://github.com/mobiletoly/sqlitenow-kmp/"
        licenses {
            license {
                name = "The Apache License, Version 2.0"
                url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                distribution = "repo"
            }
        }
        developers {
            developer {
                id = "mobiletoly"
                name = "Toly Pochkin"
                url = "https://github.com/mobiletoly"
            }
        }
        scm {
            url = "https://github.com/mobiletoly/sqlitenow-kmp"
            connection = "scm:git:git://github.com/mobiletoly/sqlitenow-kmp.git"
            developerConnection = "scm:git:git://github.com/mobiletoly/sqlitenow-kmp.git"
        }
    }
}

