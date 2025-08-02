import org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id(libs.plugins.androidLibrary.get().pluginId)
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
    id(libs.plugins.mavenPublish.get().pluginId)
}

group = "dev.goquick.sqlitenow"
version = "0.2.0-SNAPSHOT"

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

    // Produce dynamic frameworks for all iOS targets
    targets.withType<org.jetbrains.kotlin.gradle.plugin.mpp.KotlinNativeTarget>().configureEach {
        if (konanTarget.family.isAppleFamily) {
            binaries.framework("SqliteNowCore") {
                isStatic = false
                baseName = "SqliteNowCore"
            }
        }
    }

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

// Task to assemble an XCFramework containing:
//   • iosArm64 (device)
//   • iosX64   (Intel simulator)
//   • iosSimulatorArm64 (Apple Silicon simulator)
tasks.register<Exec>("assembleXCFramework") {
    group = "build"
    description = "Create XCFramework for all iOS targets"
    dependsOn(
        "linkIosArm64ReleaseFramework",
        "linkIosX64ReleaseFramework",
        "linkIosSimulatorArm64ReleaseFramework"
    )
    // Adjust paths/build types as needed
    commandLine(
        "xcodebuild",
        "-create-xcframework",
        "-framework", "build/bin/iosArm64/releaseFramework/SqliteNowCore.framework",
        "-framework", "build/bin/iosX64/releaseFramework/SqliteNowCore.framework",
        "-framework", "build/bin/iosSimulatorArm64/releaseFramework/SqliteNowCore.framework",
        "-output", "build/XCFrameworks/SqliteNowCore.xcframework"
    )
}

// Ensure the XCFramework is built before any Maven publish
tasks.withType<PublishToMavenRepository>().configureEach {
    dependsOn("assembleXCFramework")
}

// Task to build session extension for Android
tasks.register("buildSessionExtensionAndroid") {
    group = "build"
    description = "Build SQLite session extension for Android"

    doLast {
        exec {
            workingDir = rootProject.projectDir
            commandLine = listOf("bash", "native-src/android/build-session-extension.sh")
        }
    }
}

// Task to build session extension for iOS
tasks.register("buildSessionExtensionIOS") {
    group = "build"
    description = "Build SQLite session extension for iOS"

    doLast {
        exec {
            workingDir = rootProject.projectDir
            commandLine = listOf("bash", "native-src/ios/build-session-extension.sh")
        }
    }
}

// Task to build session extension for all platforms
tasks.register("buildSessionExtension") {
    group = "build"
    description = "Build SQLite session extension for all platforms"
    dependsOn("buildSessionExtensionAndroid", "buildSessionExtensionIOS")
}

// Make sure session extension is built before compilation
tasks.named("preBuild") {
    dependsOn("buildSessionExtensionAndroid")
}

// For iOS builds, ensure the iOS extension is built
tasks.matching { it.name.contains("iosArm64") || it.name.contains("iosX64") || it.name.contains("iosSimulatorArm64") }.configureEach {
    dependsOn("buildSessionExtensionIOS")
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

