{
  description = "Kubernetes API exposed as a FUSE";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";

    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, rust-overlay, ... }:
    let
      supportedSystems = [
        "x86_64-linux"
      ];

      overlays = [
        rust-overlay.overlay
      ];

      genSystems = nixpkgs.lib.genAttrs supportedSystems;
      genSystemsWithPkgs = f: genSystems (system: f (import nixpkgs { inherit system overlays; }));
    in
    {
      devShell = genSystemsWithPkgs (pkgs: pkgs.mkShell {
        buildInputs = [
          # pkgs.fuse3
          pkgs.bat
          pkgs.cargo-watch
          pkgs.k9s
          pkgs.kubectl
          pkgs.openssl
          pkgs.pkg-config
          pkgs.rust-bin.stable.latest.default
          pkgs.tree
          pkgs.yq
        ];

        F8S_LOG = "debug";
        RUST_BACKTRACE = "full";
      });
    };
}
