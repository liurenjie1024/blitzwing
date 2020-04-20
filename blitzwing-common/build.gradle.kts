import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
    java
    id("com.google.protobuf") version "0.8.12"
}

dependencies {
    implementation(rootProject.extra["depArrowMemory"] as String)
    implementation(rootProject.extra["depArrowVector"] as String)

    compileOnly(rootProject.extra["depProtobuf"] as String)
}


java {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${rootProject.extra["defProtobufVersion"]}"
    }
}

sourceSets {
    main {
        java {
//            srcDir(file("build/generated/source/proto/main/java"))
            srcDir(file("/Users/renliu/Workspace/blitzwing/blitzwing-common/build/generated/source/proto/main/java"))
        }
        proto {
            srcDir("${rootDir}/proto")
        }
    }
}
