import org.gradle.api.tasks.Exec
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
    id("dev.goquick.sqlitenow")
}

kotlin {
    jvmToolchain(17)
    applyDefaultHierarchyTemplate()

    compilerOptions {
        languageVersion.set(KotlinVersion.KOTLIN_2_4)
        freeCompilerArgs.add("-Xexpect-actual-classes")
    }

    jvm()
    macosArm64()
    iosArm64()
    iosSimulatorArm64()

    sourceSets {
        commonMain.dependencies {
            implementation(project(":library-oversqlite"))
        }
    }
}

val fixtureSwiftPackageName = "SyncFixtureDatabaseSQLiteNow"
val fixtureSwiftTargetName = "SyncFixtureDatabaseSQLiteNow"
val fixtureRuntimeModuleName = "SQLiteNowSyncRuntime"
val swiftPackageOutputDir = layout.projectDirectory.dir("../build/swift-package/$fixtureSwiftPackageName")
val swiftGeneratedSourceDir = layout.buildDirectory.dir("generated/sqlitenow/swift-product/$fixtureSwiftTargetName")
val swiftTestSourceDir = layout.projectDirectory.dir("Tests")
val runtimeXcframeworkDir =
    project(":swift:runtime:sync").layout.buildDirectory.dir("runtime/$fixtureRuntimeModuleName.xcframework")

sqliteNow {
    databases {
        create("SyncFixtureDatabase") {
            packageName = "dev.goquick.sqlitenow.swiftfixtures.sync.db"
            oversqlite = true
            debug = false
            swiftPackage {
                packageName.set(fixtureSwiftPackageName)
                swiftTargetName.set(fixtureSwiftTargetName)
                outputDirectory.set(swiftPackageOutputDir)
                generatedSourceDirectory.set(swiftGeneratedSourceDir)
                runtimeXcframework.set(runtimeXcframeworkDir)
                runtimeTaskPath.set(":swift:runtime:sync:packageDebugRuntimeXcframework")
                runtimeModuleName.set(fixtureRuntimeModuleName)
            }
        }
    }
}

fun findExecutableOnPath(name: String): File? =
    System.getenv("PATH")
        .orEmpty()
        .split(File.pathSeparator)
        .asSequence()
        .filter { it.isNotBlank() }
        .map { File(it, name) }
        .firstOrNull { it.isFile && it.canExecute() }

tasks.register<Exec>("swiftTest") {
    group = "verification"
    description = "Runs SwiftPM tests for the generated sync fixture package without enabling the realserver XCTest."

    dependsOn(
        "packageDebugSwiftPackage",
        "validateDebugSwiftPackageManifest",
        "checkDebugSwiftPackageLeaks",
    )

    inputs.file(layout.projectDirectory.file("Package.swift"))
    inputs.dir(swiftPackageOutputDir)
    inputs.dir(swiftTestSourceDir)
    inputs.dir(runtimeXcframeworkDir)
    outputs.upToDateWhen { false }

    workingDir = projectDir
    executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
    args("test")
}

tasks.register<Exec>("swiftRealserverSmoke") {
    group = "verification"
    description = "Runs the generated Swift sync package realserver smoke against go-oversync/examples/nethttp_server."

    dependsOn(
        "packageDebugSwiftPackage",
        "validateDebugSwiftPackageManifest",
        "checkDebugSwiftPackageLeaks",
    )

    inputs.file(layout.projectDirectory.file("Package.swift"))
    inputs.dir(swiftPackageOutputDir)
    inputs.dir(swiftTestSourceDir)
    inputs.dir(runtimeXcframeworkDir)
    outputs.upToDateWhen { false }

    workingDir = projectDir
    executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
    args("test")
    environment("OVERSQLITE_REALSERVER_TESTS", "true")
    environment(
        "OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL",
        System.getenv("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL")
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?: "http://localhost:8080",
    )
}
