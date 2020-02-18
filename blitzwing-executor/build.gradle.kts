import com.google.protobuf.gradle.*

plugins {
    java
    scala
    `maven-publish`
    id("me.champeau.gradle.jmh") version "0.4.8"
    id("com.google.protobuf") version "0.8.10"
}

group = "com.ebay.hadoop"
version = "0.0.1-SNAPSHOT"

//tasks.register<Copy>("copyNativeToResources") {
//    val filename = "libarrow_executor_rs${rootProject.extra["dylibSuffix"]}"
//
//    val sourceTargetDir = "${rootDir}/arrow-executor-rust/target/"
//    val sourceDir = if (rootProject.extra["release"] as Boolean) {
//        sourceTargetDir  + "release"
//    } else {
//        sourceTargetDir + "debug"
//    }
//
//    from(sourceDir)
//    include(filename)
//    into("${buildDir}/resources/main/")
//    dependsOn(":arrow-executor-rust:cargoBuild")
//}

//java {
//    sourceCompatibility = JavaVersion.VERSION_1_8
//    tasks.compileJava {
//        options.compilerArgs.addAll(listOf("-h", file("build/generated/headers").absolutePath))
//        dependsOn("copyNativeToResources")
//    }
//    tasks.test {
//        testLogging.showStandardStreams = true
//    }
//}


repositories {
    mavenCentral()
}



val scalaVersion = "2.12.10"
val scalaBinaryVersion= "2.12"
val arrowVersion = "0.15.1"
val jmhVersion = "1.21"
val protobufVersion = "2.5.0"
val parquet4sVersion = "0.11.0"


dependencies {
    "jmh"("org.openjdk.jmh:jmh-core:${jmhVersion}")
    "jmh"("org.openjdk.jmh:jmh-generator-annprocess:${jmhVersion}")

    "compile"("org.scala-lang:scala-library:${scalaVersion}")
    "compile"("org.apache.arrow:arrow-vector:${arrowVersion}")
    "compile"("org.apache.arrow:arrow-memory:${arrowVersion}")
    "compile"("com.google.protobuf:protobuf-java:${protobufVersion}")
    "compile"("com.github.mjakubowski84:parquet4s-akka_${scalaBinaryVersion}:${parquet4sVersion}")
    "compile"(files("${rootDir}/arrow-executor-rust/target/debug/libarrow_executor_rs.dylib"))

    "testImplementation"("junit:junit:4.12")
    "testImplementation"("org.scalatest:scalatest_${scalaBinaryVersion}:3.0.5")
    "testImplementation"("org.apache.hadoop:hadoop-client:2.7.3")
    "testImplementation"("commons-io:commons-io:2.5")
    "testImplementation"("com.google.protobuf:protobuf-java-util:3.0.0")
}

//sourceSets {
//    main {
//        java {
//            srcDir(file("build/generated/source/proto/main/java"))
//        }
//        proto {
//            srcDir(rootProject.extra["protoDir"] as File)
//        }
//    }
//}
//
//
//publishing {
//    publications {
//        create<MavenPublication>("maven") {
//            artifactId = "arrow-executor"
//            from(components["java"])
//        }
//    }
//}
//
//protobuf {
//    // Configure the protoc executable
//    protoc {
//        // Download from repositories
//        artifact = "com.google.protobuf:protoc:${protobufVersion}"
//    }
//}

