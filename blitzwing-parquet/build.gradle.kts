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
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

