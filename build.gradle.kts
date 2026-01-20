import org.jetbrains.kotlin.gradle.targets.js.nodejs.NodeJsEnvSpec
import org.jetbrains.kotlin.gradle.targets.js.nodejs.NodeJsRootPlugin

plugins {
    alias(libs.plugins.androidLibrary) apply false
    alias(libs.plugins.androidApplication) apply false
    alias(libs.plugins.androidKotlinMultiplatformLibrary) apply false
    alias(libs.plugins.jetbrainsCompose) apply false
    alias(libs.plugins.composeCompiler) apply false
    alias(libs.plugins.kotlinMultiplatform) apply false
    alias(libs.plugins.mavenPublish) apply false
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.serialization) apply false
    alias(libs.plugins.kotlin.android) apply false
}

// Kotlin/JS uses Yarn v1 internally; with Node 24+ it emits `url.parse()` deprecation warnings during `kotlinNpmInstall`.
// Pin Node to LTS for quieter, reproducible builds.
plugins.withType<NodeJsRootPlugin> {
    extensions.configure<NodeJsEnvSpec> {
        version.convention("22.0.0")
    }
}
