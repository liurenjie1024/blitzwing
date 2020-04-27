plugins {
    java
    application
}

tasks.register<Copy>("copyNativeToResources") {
    val profile = if (rootProject.hasProperty("release")) "release" else "debug"
    val sourceDir = "${rootDir}/blitzwing-native/target/${profile}"
    val filename = System.mapLibraryName("blitzwing_rs")

    from(sourceDir)
    include(filename)
    into("${projectDir}/src/main/resources/")
    dependsOn(":blitzwing-native:build")
}

dependencies {
    implementation(project(":blitzwing-common"))
    compileOnly(rootProject.extra["depProtobuf"] as String)
    implementation(rootProject.extra["depHadoopClient"] as String)
    implementation(rootProject.extra["depParquetHadoop"] as String)
    implementation(rootProject.extra["depParquetColumn"] as String)
    implementation(rootProject.extra["depArrowVector"] as String)
    implementation(rootProject.extra["depArrowMemory"] as String)
    testImplementation(rootProject.extra["depJunit"] as String)
    testImplementation(rootProject.extra["depParquetAvro"] as String)
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    tasks.compileJava {
        options.compilerArgs.addAll(listOf("-h", file("build/generated/headers").absolutePath))
        dependsOn("copyNativeToResources")
    }
    tasks.test {
        testLogging.showStandardStreams = true
    }
}

application {
    mainClassName = "com.ebay.hadoop.blitzwing.arrow.adaptor.parquet.Demo"
}

