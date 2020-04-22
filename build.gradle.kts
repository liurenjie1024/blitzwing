val scalaBinaryVersion = "2.12"
val scalaVersion = "2.12.10"
val hadoopVersion = "2.7.3.2.6.4.1.0.12"
val junitVersion = "4.12"
val parquetVersion = "1.8.3-encryption-ebay0.1-SNAPSHOT"
val arrowVersion = "0.15.1"
val protobufVersion = "2.5.0"
val parquet4sVersion = "1.0.0"


val depScalaLang by project.extra("org.scala-lang:scala-library:${scalaVersion}")
val depHadoopCommon by project.extra("org.apache.hadoop:hadoop-common:${hadoopVersion}")
val depHadoopYarnApi by project.extra("org.apache.hadoop:hadoop-yarn-api:${hadoopVersion}")
val depHadoopHdfs by project.extra("org.apache.hadoop:hadoop-hdfs:${hadoopVersion}")
val depHadoopClient by project.extra("org.apache.hadoop:hadoop-client:${hadoopVersion}")
val depParquetHadoop by project.extra("org.apache.parquet:parquet-hadoop:${parquetVersion}")
val depParquetColumn by project.extra("org.apache.parquet:parquet-column:${parquetVersion}")
val depParquetAvro by project.extra("org.apache.parquet:parquet-avro:${parquetVersion}")
val depArrowVector by project.extra("org.apache.arrow:arrow-vector:${arrowVersion}")
val depArrowMemory by project.extra("org.apache.arrow:arrow-memory:${arrowVersion}")
val depParquet4s by project.extra("com.github.mjakubowski84:parquet4s_${scalaBinaryVersion}:${scalaVersion}")
val depProtobuf by project.extra("com.google.protobuf:protobuf-java:${protobufVersion}")
val depJunit by project.extra("junit:junit:${junitVersion}")

val defProtobufVersion by project.extra(protobufVersion)

allprojects {
    group = "com.ebay.hadoop"
    version = "1.0-SNAPSHOT"

    repositories {
        mavenLocal()
        maven { url = uri("https://ebaycentral.qa.ebay.com/content/repositories/releases/") }
        maven { url = uri("https://ebaycentral.qa.ebay.com/content/repositories/v3debt/") }
        maven { url = uri("https://ebaycentral.qa.ebay.com/content/repositories/snapshots/") }
        maven { url = uri("https://ebaycentral.qa.ebay.com/content/repositories/thirdparty") }
        maven { url = uri("https://ebaycentral.qa.ebay.com/content/repositories/commercial") }
        maven { url = uri("https://maven.twttr.com/") }
        maven { url = uri("https://oss.sonatype.org/content/repositories/snapshots") }
        maven { url = uri("https://repository.apache.org/content/groups/public") }
        maven { url = uri("https://repository.apache.org/content/repositories/releases/") }
        maven { url = uri("https://repo.hortonworks.com/content/groups/public") }
        maven { url = uri("https://clojars.org/repo")}
        mavenCentral()
    }
}

