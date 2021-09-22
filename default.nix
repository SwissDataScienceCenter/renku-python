# This defines the nix build for renku.
#
# To build the renku package:
#
# $ nix-build
#
# To get a shell with renku and all of its runtime dependencies inside:
#
# $ nix-shell
#
# To get a shell with _only_ the renku runtime (ignoring the host system):
#
# $ nix-shell --pure
#
# For the shell definition see ./shell.nix

{ system ? builtins.currentSystem, version ? null }:
let
    pkgs = import <nixpkgs> { inherit system; };
    mach-nix = import (builtins.fetchGit {
        url = "https://github.com/uwmisl/mach-nix";
        ref = "master";
    }) {
      inherit pkgs;
      pypiDataRev = "6ad33b03549c6b0882d8c3d826ec7fbef84ecec4";
      pypiDataSha256 = "0bk78imn5wf50frnsyn654l0wmz77g4n79gxnqvalp80mzj7dg1k";
    };

in with pkgs;
    mach-nix.buildPythonPackage {
    pname = "renku";
    version = "0.16.0";

    src = ./.;
    extras = [ "all" ];
    requirementsExtra = ''
        setuptools_rust
        pyyaml
        pytest
    '';

    nativeBuildInputs = [ git git-lfs nodejs ];
    propagatedBuildInputs = [ git git-lfs nodejs ];
    _.apispec.propagatedBuildInputs.mod = pySelf: self: oldVal: oldVal ++ [ pySelf.pyyaml ];
    GIT_SSL_NO_VERIFY = "true";
    SETUPTOOLS_SCM_PRETEND_VERSION = version;
}
