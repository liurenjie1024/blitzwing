plugins {
    java
    scala
}

group = "com.ebay.hadoop"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        url = uri("http://ebaycentral.qa.ebay.com/content/repositories/releases/")
    }
    maven {
        url = uri("http://ebaycentral.qa.ebay.com/content/repositories/v3debt/")
    }
    maven {
        url = uri("http://ebaycentral.qa.ebay.com/content/repositories/snapshots/")
    }
    maven {
        url = uri("http://ebaycentral.qa.ebay.com/content/repositories/thirdparty")
    }
    maven {
        url = uri("http://ebaycentral.qa.ebay.com/content/repositories/commercial")
    }
}

val parquetVersion = "1.8.3-encryption-ebay0.1-SNAPSHOT"
val arrowVersion = "0.15.1"

dependencies {
    compile("org.apache.parquet", "parquet-hadoop", parquetVersion)
    compile("org.apache.parquet", "parquet-column", parquetVersion)
    compile("org.apache.arrow", "arrow-vector", arrowVersion)
    compile("org.apache.arrow", "arrow-memory", arrowVersion)
    testCompile("junit", "junit", "4.12")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}