plugins {
    kotlin("jvm") version "2.0.21"
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("org.json:json:20240303")
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("chatango.MainKt")
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}
