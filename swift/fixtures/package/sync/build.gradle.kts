import org.gradle.api.tasks.Exec
import dev.goquick.sqlitenow.gradle.GenerateDatabaseFilesTask
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
val coordinatedopeningGenerationOnePackageName = "CoordinatedOpeningGenerationOneDatabaseSQLiteNow"
val coordinatedopeningGenerationTwoPackageName = "CoordinatedOpeningGenerationTwoDatabaseSQLiteNow"
val schemaevolutionGenerationOnePackageName = "SchemaEvolutionGenerationOneDatabaseSQLiteNow"
val schemaevolutionLocalTwoPackageName = "SchemaEvolutionLocalTwoDatabaseSQLiteNow"
val schemaevolutionGenerationTargetPackageName = "SchemaEvolutionGenerationTargetDatabaseSQLiteNow"
val schemaevolutionInvalidTargetPackageName = "SchemaEvolutionInvalidTargetDatabaseSQLiteNow"

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
        create("CoordinatedOpeningGenerationOneDatabase") {
            packageName = "dev.goquick.sqlitenow.swiftfixtures.sync.coordinatedopening.generationone"
            oversqlite = true
            debug = false
            swiftPackage {
                packageName.set(coordinatedopeningGenerationOnePackageName)
                swiftTargetName.set(coordinatedopeningGenerationOnePackageName)
                outputDirectory.set(layout.projectDirectory.dir("../build/swift-package/$coordinatedopeningGenerationOnePackageName"))
                generatedSourceDirectory.set(
                    layout.buildDirectory.dir("generated/sqlitenow/swift-product/$coordinatedopeningGenerationOnePackageName"),
                )
                runtimeXcframework.set(runtimeXcframeworkDir)
                runtimeTaskPath.set(":swift:runtime:sync:packageDebugRuntimeXcframework")
                runtimeModuleName.set(fixtureRuntimeModuleName)
            }
        }
        create("CoordinatedOpeningGenerationTwoDatabase") {
            packageName = "dev.goquick.sqlitenow.swiftfixtures.sync.coordinatedopening.generationtwo"
            oversqlite = true
            debug = false
            swiftPackage {
                packageName.set(coordinatedopeningGenerationTwoPackageName)
                swiftTargetName.set(coordinatedopeningGenerationTwoPackageName)
                outputDirectory.set(layout.projectDirectory.dir("../build/swift-package/$coordinatedopeningGenerationTwoPackageName"))
                generatedSourceDirectory.set(
                    layout.buildDirectory.dir("generated/sqlitenow/swift-product/$coordinatedopeningGenerationTwoPackageName"),
                )
                runtimeXcframework.set(runtimeXcframeworkDir)
                runtimeTaskPath.set(":swift:runtime:sync:packageDebugRuntimeXcframework")
                runtimeModuleName.set(fixtureRuntimeModuleName)
            }
        }
        listOf(
            "SchemaEvolutionGenerationOneDatabase" to "dev.goquick.sqlitenow.swiftfixtures.sync.schemaevolution.generationone",
            "SchemaEvolutionLocalTwoDatabase" to "dev.goquick.sqlitenow.swiftfixtures.sync.schemaevolution.localtwo",
            "SchemaEvolutionGenerationTargetDatabase" to "dev.goquick.sqlitenow.swiftfixtures.sync.schemaevolution.target",
            "SchemaEvolutionInvalidTargetDatabase" to "dev.goquick.sqlitenow.swiftfixtures.sync.schemaevolution.invalidtarget",
        ).forEach { (databaseName, kotlinPackage) ->
            create(databaseName) {
                packageName = kotlinPackage
                oversqlite = true
                debug = false
                swiftPackage {
                    packageName.set("${databaseName}SQLiteNow")
                    swiftTargetName.set("${databaseName}SQLiteNow")
                    outputDirectory.set(layout.projectDirectory.dir("../build/swift-package/${databaseName}SQLiteNow"))
                    generatedSourceDirectory.set(
                        layout.buildDirectory.dir("generated/sqlitenow/swift-product/${databaseName}SQLiteNow"),
                    )
                    runtimeXcframework.set(runtimeXcframeworkDir)
                    runtimeTaskPath.set(":swift:runtime:sync:packageDebugRuntimeXcframework")
                    runtimeModuleName.set(fixtureRuntimeModuleName)
                }
            }
        }
    }
}

val sharedSchemaEvolutionSqlRoot = rootProject.layout.projectDirectory.dir("platform-oversqlite-test/composeApp/src/commonMain/sql")
tasks.withType<GenerateDatabaseFilesTask>().configureEach {
    if (listOf(
            "SchemaEvolutionGenerationOneDatabase",
            "SchemaEvolutionLocalTwoDatabase",
            "SchemaEvolutionGenerationTargetDatabase",
            "SchemaEvolutionInvalidTargetDatabase",
        ).any(name::contains)
    ) {
        sqlDir.set(sharedSchemaEvolutionSqlRoot)
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
        "packageSyncFixtureDatabaseDebugSwiftPackage",
        "validateSyncFixtureDatabaseDebugSwiftPackageManifest",
        "checkSyncFixtureDatabaseDebugSwiftPackageLeaks",
        "packageCoordinatedOpeningGenerationOneDatabaseDebugSwiftPackage",
        "validateCoordinatedOpeningGenerationOneDatabaseDebugSwiftPackageManifest",
        "checkCoordinatedOpeningGenerationOneDatabaseDebugSwiftPackageLeaks",
        "packageCoordinatedOpeningGenerationTwoDatabaseDebugSwiftPackage",
        "validateCoordinatedOpeningGenerationTwoDatabaseDebugSwiftPackageManifest",
        "checkCoordinatedOpeningGenerationTwoDatabaseDebugSwiftPackageLeaks",
        "packageSchemaEvolutionGenerationOneDatabaseDebugSwiftPackage",
        "validateSchemaEvolutionGenerationOneDatabaseDebugSwiftPackageManifest",
        "checkSchemaEvolutionGenerationOneDatabaseDebugSwiftPackageLeaks",
        "packageSchemaEvolutionLocalTwoDatabaseDebugSwiftPackage",
        "validateSchemaEvolutionLocalTwoDatabaseDebugSwiftPackageManifest",
        "checkSchemaEvolutionLocalTwoDatabaseDebugSwiftPackageLeaks",
        "packageSchemaEvolutionGenerationTargetDatabaseDebugSwiftPackage",
        "validateSchemaEvolutionGenerationTargetDatabaseDebugSwiftPackageManifest",
        "checkSchemaEvolutionGenerationTargetDatabaseDebugSwiftPackageLeaks",
        "packageSchemaEvolutionInvalidTargetDatabaseDebugSwiftPackage",
        "validateSchemaEvolutionInvalidTargetDatabaseDebugSwiftPackageManifest",
        "checkSchemaEvolutionInvalidTargetDatabaseDebugSwiftPackageLeaks",
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
        "packageSyncFixtureDatabaseDebugSwiftPackage",
        "validateSyncFixtureDatabaseDebugSwiftPackageManifest",
        "checkSyncFixtureDatabaseDebugSwiftPackageLeaks",
        "packageCoordinatedOpeningGenerationOneDatabaseDebugSwiftPackage",
        "validateCoordinatedOpeningGenerationOneDatabaseDebugSwiftPackageManifest",
        "checkCoordinatedOpeningGenerationOneDatabaseDebugSwiftPackageLeaks",
        "packageCoordinatedOpeningGenerationTwoDatabaseDebugSwiftPackage",
        "validateCoordinatedOpeningGenerationTwoDatabaseDebugSwiftPackageManifest",
        "checkCoordinatedOpeningGenerationTwoDatabaseDebugSwiftPackageLeaks",
        "packageSchemaEvolutionGenerationOneDatabaseDebugSwiftPackage",
        "validateSchemaEvolutionGenerationOneDatabaseDebugSwiftPackageManifest",
        "checkSchemaEvolutionGenerationOneDatabaseDebugSwiftPackageLeaks",
        "packageSchemaEvolutionLocalTwoDatabaseDebugSwiftPackage",
        "validateSchemaEvolutionLocalTwoDatabaseDebugSwiftPackageManifest",
        "checkSchemaEvolutionLocalTwoDatabaseDebugSwiftPackageLeaks",
        "packageSchemaEvolutionGenerationTargetDatabaseDebugSwiftPackage",
        "validateSchemaEvolutionGenerationTargetDatabaseDebugSwiftPackageManifest",
        "checkSchemaEvolutionGenerationTargetDatabaseDebugSwiftPackageLeaks",
        "packageSchemaEvolutionInvalidTargetDatabaseDebugSwiftPackage",
        "validateSchemaEvolutionInvalidTargetDatabaseDebugSwiftPackageManifest",
        "checkSchemaEvolutionInvalidTargetDatabaseDebugSwiftPackageLeaks",
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
