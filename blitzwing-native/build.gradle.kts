//tasks.register<Exec>("generateProtobuf") {
//    commandLine("protoc", "--rust_out", file("src/proto").absolutePath, "--proto_path",
//            (rootProject.extra["protoDir"] as File).absolutePath,
//            *(rootProject.extra["protoFiles"] as Array<String>))
//}

tasks.register<Exec>("build") {
    val args = mutableListOf("cargo", "build", "-v")
    if (rootProject.hasProperty("release")) {
        args.add("--release")
    }
    commandLine(*(args.toTypedArray()))
}

tasks.register<Exec>("clean") {
    val args = mutableListOf("cargo", "clean")
    commandLine(*(args.toTypedArray()))
}

tasks.register<Exec>("test") {
    val args = mutableListOf("cargo", "test")
    commandLine(*(args.toTypedArray()))
}

//tasks["test"].dependsOn("cargoTest")
//tasks["build"].dependsOn("cargoBuild")
//tasks["clean"].dependsOn("cargoClean")