import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("org.jetbrains.kotlin.jvm").version("1.7.20")
    id("com.github.johnrengelman.shadow").version("7.1.2")
}

group = "com.github.exerosis.rabia"
version = "1.0.0"

repositories {
    maven("https://repo1.maven.org/maven2/")
    mavenLocal();maven("https://jitpack.io")
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")
    implementation("com.github.exerosis.mynt:Mynt:1.0.11")
}

tasks.shadowJar {
    archiveFileName.set("${project.name}.jar")
    destinationDirectory.set(file("./"))
    manifest.attributes["Main-Class"] = "com.github.exerosis.rabia.MainKt"
}

val client = tasks.create<ShadowJar>("client") {
    from(sourceSets.main.get().output)
    configurations = listOf(project.configurations.runtimeClasspath.get())
    archiveFileName.set("Client.jar")
    destinationDirectory.set(file("./"))
    manifest.attributes["Main-Class"] = "com.github.exerosis.rabia.ClientKt"
}

tasks.build { dependsOn(tasks.shadowJar, client) }