# This builds a shell with renku installed from source from this directory.
# The shell also includes git, git-lfs and nodejs.
#
# Usage:
#
# $ nix-shell
#
# To only use packages defined in this shell and ignore system packages:
#
# $ nix-shell --pure
#
{ system ? builtins.currentSystem }:
let
    pkgs = import <nixpkgs> { inherit system; };
    renku = import ./default.nix {};
in with pkgs;
    mkShell {
        name = "renku-dev-env";
        buildInputs = [ renku ];
    }
