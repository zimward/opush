{
  description = "A very basic flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs =
    { self, ... }@inputs:
    {
      packages = builtins.mapAttrs (
        system: pkgs:
        let
          lib = inputs.nixpkgs.lib;
        in
        {
          default = pkgs.rustPlatform.buildRustPackage {
            name = "opush";
            version = "0.1.0";
            src = lib.cleanSource ./.;
            cargoLock.lockFile = ./Cargo.lock;

            strictDeps = true;
            __structuredAttrs = true;

          };
        }
      ) inputs.nixpkgs.legacyPackages;
      devShells = builtins.mapAttrs (system: pkgs: {
        default = pkgs.mkShell {
          buildInputs = with pkgs; [
            rustPlatform.rust.rustc
            rustPlatform.rust.cargo
            gdb
            clippy
            rustfmt
            rust-analyzer
            zbus-xmlgen
          ];
        };
      }) inputs.nixpkgs.legacyPackages;
    };
}
