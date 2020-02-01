extern crate protoc_rust_grpc;

fn main() {
    // Generate Protobuf shims
    let protobuf_sources = &["protobuf/raft.proto", "protobuf/toydb.proto"];

    protoc_rust_grpc::run(protoc_rust_grpc::Args {
        input: protobuf_sources,
        out_dir: "src/service",
        includes: &[],
        rust_protobuf: true,
    })
    .expect("protoc-rust-grpc");

    for src in protobuf_sources {
        println!("cargo:rerun-if-changed={}", src);
    }
}
