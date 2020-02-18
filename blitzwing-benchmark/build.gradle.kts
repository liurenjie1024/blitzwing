plugins {
    java
    scala
    application
}

group = "com.ebay.hadoop"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

application {
    mainClassName = "com.ebay.hadoop.spark.BenchMark"
}