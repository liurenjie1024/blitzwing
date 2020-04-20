plugins {
    java
}


dependencies {
    implementation(project(":blitzwing-common"))
    compileOnly(rootProject.extra["depProtobuf"] as String)
    implementation(rootProject.extra["depParquetHadoop"] as String)
    implementation(rootProject.extra["depParquetColumn"] as String)
    implementation(rootProject.extra["depArrowVector"] as String)
    implementation(rootProject.extra["depArrowMemory"] as String)
    testImplementation(rootProject.extra["depJunit"] as String)
    testImplementation(rootProject.extra["depHadoopClient"] as String)
    testImplementation(rootProject.extra["depParquetAvro"] as String)
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    tasks.compileJava {
        options.compilerArgs.addAll(listOf("-h", file("build/generated/headers").absolutePath))
    }
    tasks.test {
        testLogging.showStandardStreams = true
    }
}

