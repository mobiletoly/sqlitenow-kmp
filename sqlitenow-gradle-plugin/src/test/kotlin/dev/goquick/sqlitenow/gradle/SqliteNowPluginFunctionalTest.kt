package dev.goquick.sqlitenow.gradle

import java.io.File
import java.nio.file.Path
import org.gradle.testkit.runner.GradleRunner
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import kotlin.io.path.invariantSeparatorsPathString
import kotlin.test.assertTrue

class SqliteNowPluginFunctionalTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    @DisplayName("Plugin generates and compiles migration code for a real project fixture")
    fun pluginGeneratesAndCompilesMigrationCode() {
        val repoRoot = resolveRepoRoot()
        val pluginBuild = repoRoot.resolve("sqlitenow-gradle-plugin")
        val projectDir = tempDir.resolve("fixture-project").toFile().apply { mkdirs() }

        File(projectDir, "settings.gradle.kts").writeText(
            """
                pluginManagement {
                    repositories {
                        google()
                        mavenCentral()
                        gradlePluginPortal()
                    }
                    includeBuild("${pluginBuild.invariantSeparatorsPathString}")
                }

                dependencyResolutionManagement {
                    repositories {
                        google()
                        mavenCentral()
                    }
                }

                includeBuild("${repoRoot.invariantSeparatorsPathString}") {
                    dependencySubstitution {
                        substitute(module("dev.goquick.sqlitenow:core")).using(project(":library"))
                    }
                }

                rootProject.name = "sqlite-now-plugin-fixture"
            """.trimIndent()
        )

        File(projectDir, "build.gradle.kts").writeText(
            """
                import org.jetbrains.kotlin.gradle.dsl.JvmTarget
                import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

                plugins {
                    kotlin("multiplatform") version "2.3.20"
                    id("dev.goquick.sqlitenow")
                }

                group = "fixture"
                version = "1.0.0"

                repositories {
                    google()
                    mavenCentral()
                }

                kotlin {
                    jvm()
                    jvmToolchain(17)

                    compilerOptions {
                        languageVersion.set(KotlinVersion.KOTLIN_2_3)
                    }

                    sourceSets {
                        commonMain.dependencies {
                            implementation("dev.goquick.sqlitenow:core:0.7.0-SNAPSHOT")
                        }
                    }
                }

                tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile>().configureEach {
                    compilerOptions {
                        jvmTarget.set(JvmTarget.JVM_17)
                    }
                }

                sqliteNow {
                    databases {
                        create("FixtureDatabase") {
                            packageName = "fixture.db"
                            debug = false
                        }
                    }
                }
            """.trimIndent()
        )

        File(projectDir, "src/commonMain/kotlin/fixture/FakeUsage.kt").apply {
            parentFile.mkdirs()
            writeText(
                """
                    package fixture

                    class FakeUsage
                """.trimIndent()
            )
        }

        writeSqlFixture(projectDir)

        GradleRunner.create()
            .withProjectDir(projectDir)
            .withArguments("compileKotlinJvm", "--stacktrace")
            .forwardOutput()
            .build()

        val generatedFile = projectDir.resolve(
            "build/generated/sqlitenow/code/FixtureDatabase/fixture/db/VersionBasedDatabaseMigrations.kt"
        )
        assertTrue(generatedFile.exists(), "Generated migration file should exist")
        assertTrue(
            generatedFile.readText().contains("private suspend fun migrateToVersion1"),
            "Generated migration helper should be suspend in fixture build"
        )
    }

    private fun writeSqlFixture(projectDir: File) {
        val dbRoot = projectDir.resolve("src/commonMain/sql/FixtureDatabase")
        File(dbRoot, "schema/person.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    CREATE TABLE person (
                        id INTEGER PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL,
                        email TEXT
                    );
                """.trimIndent()
            )
        }
        File(dbRoot, "queries/person/selectAll.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    SELECT *
                    FROM person
                    ORDER BY id;
                """.trimIndent()
            )
        }
        File(dbRoot, "migration/0001.sql").apply {
            parentFile.mkdirs()
            writeText("ALTER TABLE person ADD COLUMN email TEXT;")
        }
    }

    private fun resolveRepoRoot(): Path {
        val cwd = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize()
        return if (cwd.resolve("library").toFile().exists() && cwd.resolve("sqlitenow-gradle-plugin").toFile().exists()) {
            cwd
        } else {
            cwd.parent
        }
    }
}
