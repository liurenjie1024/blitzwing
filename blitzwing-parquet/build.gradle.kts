plugins {
    java
}


dependencies {
    implementation(rootProject.extra["depParquetHadoop"] as String)
    implementation(rootProject.extra["depParquetColumn"] as String)
    implementation(rootProject.extra["depArrowVector"] as String)
    implementation(rootProject.extra["depArrowMemory"] as String)
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}