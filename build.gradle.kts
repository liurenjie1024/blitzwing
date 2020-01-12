group = "com.ebay.hadoop"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

subprojects {
    apply(plugin = "java")
}

ext {
    extra["release"] = hasProperty("release")
//    extra["dylibSuffix"] = if (org.apache.tools.ant.taskdefs.condition.Os.isFamily(org.apache.tools.ant.taskdefs.condition.Os.FAMILY_MAC)) {
//        ".dylib"
//    } else {
//        ".so"
//    }
//
//    val protoDir = file("${rootDir}/proto")
//    extra["protoDir"] = protoDir
//    extra["protoFiles"] = protoDir.listFiles { file: File -> file.isFile && file.name.endsWith(".proto") }
//            .map { f: File -> f.name }
//            .toTypedArray()
}
