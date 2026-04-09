import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.testing.Test

plugins {
    kotlin("jvm") version "2.2.21"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "me.jaehyeon"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val flinkVersion = "1.20.1"
val moaVersion = "2024.07.0"

// ==========================================
// MEND.IO VULNERABILITY OVERRIDES
// ==========================================
configurations.all {
    resolutionStrategy {
        // Force patch Jackson (Fixes GHSA-72hv-8253-57qq / WS-2026-0003)
        force("com.fasterxml.jackson.core:jackson-core:2.21.1")
        force("com.fasterxml.jackson.core:jackson-databind:2.21.1")
        force("com.fasterxml.jackson.core:jackson-annotations:2.21")

        // Force patch Log4j (Fixes CVE-2025-68161)
        force("org.apache.logging.log4j:log4j-api:2.25.4")
        force("org.apache.logging.log4j:log4j-core:2.25.4")
        force("org.apache.logging.log4j:log4j-slf4j-impl:2.25.4")

        // Force patch Commons Lang (Fixes CVE-2025-48924)
        force("org.apache.commons:commons-lang3:3.18.0")

        // Force patch Plexus Utils (Fixes CVE-2025-67030)
        force("org.codehaus.plexus:plexus-utils:4.0.3")

        // Force patch LZ4 (Fixes CVE-2025-66566)
        force("org.lz4:lz4-java:1.8.1")

        // Force patch Kafka Clients (Fixes CVE-2026-35554)
        force("org.apache.kafka:kafka-clients:3.9.2")
    }

    // Aggressively exclude vulnerable transitive libraries we don't actually use
    exclude(group = "commons-beanutils", module = "commons-beanutils")
    exclude(group = "ai.djl", module = "api")

    // Standard logging exclusions to prevent classpath collisions
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
    exclude(group = "org.slf4j", module = "slf4j-reload4j")
    exclude(group = "log4j", module = "log4j")
}

val localRunClasspath by configurations.creating {
    extendsFrom(configurations.implementation.get(), configurations.compileOnly.get(), configurations.runtimeOnly.get())
}

// ==========================================
// PROJECT DEPENDENCIES
// ==========================================
dependencies {
    // Flink Streaming
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")
    compileOnly("org.apache.flink:flink-connector-base:$flinkVersion")

    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-clients:$flinkVersion")
    testImplementation("org.apache.flink:flink-connector-base:$flinkVersion")

    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.9.2")
    implementation("org.apache.flink:flink-connector-kafka:3.4.0-1.20")

    // JSON Serialization (Synchronized with forced secure versions)
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.21")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.1")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.21.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.21.1")

    // MOA
    implementation("nz.ac.waikato.cms.moa:moa:$moaVersion")

    // ClickHouse Sink
    implementation("com.clickhouse.flink:flink-connector-clickhouse-1.17:0.1.3:all")

    // Logging (Synchronized with forced secure versions)
    runtimeOnly("org.apache.logging.log4j:log4j-api:2.25.4")
    runtimeOnly("org.apache.logging.log4j:log4j-core:2.25.4")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.25.4")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.14.1")
}

kotlin {
    jvmToolchain(11)
    compilerOptions {
        freeCompilerArgs.add("-Xannotation-default-target=param-property")
    }
}

application {
    mainClass.set("me.jaehyeon.hotrolling.MainKt")
}

tasks.withType<ShadowJar> {
    archiveBaseName.set(rootProject.name)
    archiveClassifier.set("")
    archiveVersion.set("1.0")

    mergeServiceFiles()

    relocate("com.fasterxml.jackson", "me.jaehyeon.shaded.jackson")

    dependencies {
        exclude(dependency("org.apache.logging.log4j:.*"))
        exclude(dependency("org.slf4j:.*"))
    }
}

tasks.named("build") {
    dependsOn("shadowJar")
}

tasks.named<JavaExec>("run") {
    classpath = localRunClasspath + sourceSets.main.get().output
    environment("BOOTSTRAP", "localhost:9092")
    environment("CH_ENDPOINT", "http://localhost:8123")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
