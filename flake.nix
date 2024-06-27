{
  description = "toydb";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable"; 
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, flake-utils, rust-overlay,... }: let
    lib = {
      inherit (flake-utils.lib) defaultSystems eachSystem;
    };
    supportedSystems = [ "x86_64-linux" ];
  in lib.eachSystem supportedSystems (system: let

    nightlyVersion = "2023-09-29";
    pkgs = import nixpkgs {
        inherit system;
        overlays = [
          (import rust-overlay)
        ];
      };
    pinnedRust = pkgs.rust-bin.nightly.${nightlyVersion}.default.override {
      extensions = ["rustc-dev" "rust-src" "rust-analyzer-preview" ];
      targets = [ "x86_64-unknown-linux-gnu" ];
    };
    rustPlatform = pkgs.makeRustPlatform {
      rustc = pinnedRust;
      cargo = pinnedRust;
    };
  in {
    
devShell = pkgs.mkShell {
  hardeningDisable = [
    "fortify"
  ];
  nativeBuildInputs = [
    pinnedRust
    pkgs.protobuf
  ];
  buildInputs = [
  ];

  shellHook = ''
  '';
};

  });
}
