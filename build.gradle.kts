
plugins { kotlin("jvm").version("1.7.20") }

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