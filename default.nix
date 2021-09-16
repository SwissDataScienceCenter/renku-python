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
{ system ? builtins.currentSystem }:
let
    pkgs = import <nixpkgs> { inherit system; };
    mach-nix = import (builtins.fetchGit {
        url = "https://github.com/uwmisl/mach-nix";
        ref = "master";
    }) {
      inherit pkgs;
      pypiDataRev = "5c6e5ecbc5a60fb9c43dc77be8e0eb8ac89f4fee";
      pypiDataSha256 = "0gnq6r92bmnhqjykx3jff7lvg7wbpayd0wvb0drra8r8dvmr5b2d";
    };

in with pkgs;
    mach-nix.buildPythonApplication {
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
    doCheck = true;
    checkPhase = ''
        python -m pytest -v -m "not integration and not publish and not service"
    '';
}
