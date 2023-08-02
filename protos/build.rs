use tonic_build::configure;

fn main() {
    configure()
        .emit_rerun_if_changed(true)
        .compile(&["protos/geyser.proto"], &["protos"])
        .unwrap();
}
