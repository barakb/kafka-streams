import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import  com.github.davidmc24.gradle.plugin.avro.*

plugins {
	id("org.springframework.boot") version "2.5.4"
	id("io.spring.dependency-management") version "1.0.11.RELEASE"
	kotlin("jvm") version "1.5.21"
	kotlin("plugin.spring") version "1.5.21"
	id("com.github.davidmc24.gradle.plugin.avro") version "1.2.1"
}

group = "com.totango"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_1_8

repositories {
	mavenCentral()
	gradlePluginPortal()
	maven{
		name = "confluent"
		url = uri("https://packages.confluent.io/maven")
	}

}

dependencies {
	implementation("org.apache.kafka:kafka-streams")
	implementation("org.apache.avro:avro:1.10.2")
	implementation("io.confluent:kafka-avro-serializer:6.2.0")
	implementation("io.confluent:kafka-streams-avro-serde:6.2.0")
	implementation("org.springframework.boot:spring-boot-starter-webflux")
	implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
	implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
	implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
	implementation("com.google.code.gson:gson:2.8.8")
	testImplementation("org.apache.kafka:kafka-streams-test-utils")
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("io.projectreactor:reactor-test")
}

tasks.withType<KotlinCompile> {
	kotlinOptions {
		freeCompilerArgs = listOf("-Xjsr305=strict")
		jvmTarget = "1.8"
	}
}

tasks.withType<Test> {
	useJUnitPlatform()
}

avro {
	fieldVisibility.set("PRIVATE")
}

//tasks.withType<GenerateAvroJavaTask>{
//	source("src/main/resources/avro")
//	setOutputDir(file("src/main/java/avro"))
//}