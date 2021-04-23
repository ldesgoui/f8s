{
  description = "Kubernetes API exposed as a FUSE";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";

    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, rust-overlay, ... }: {
    devShell.x86_64-linux =
      let pkgs =
        nixpkgs.legacyPackages.x86_64-linux;
      in
      pkgs.mkShell {
        buildInputs = [
          pkgs.fuse3
          pkgs.pkg-config
          rust-overlay.defaultPackage.x86_64-linux
        ];
      };
  };
}
