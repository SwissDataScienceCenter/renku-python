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
    cwl-upgrader = import ./cwl-upgrader/cwl-upgrader.nix;
    reqs = mach-nix.mkPython {
        requirements = ''
        setuptools_rust
        ruamel.yaml<=0.16.5,>=0.12.4
        yagup>=0.1.1
        cffi
        '';
    };
in with pkgs;
    python39.pkgs.buildPythonApplication {
    pname = "renku";
    version = "0.16.0";

    src = ./.;

    # _.cryptography.propagatedBuildInputs.mod = pySelf: self: oldVal: oldVal ++ [ pySelf.cffi ];
    buildInputs = [ cwl-upgrader reqs ];
    nativeBuildInputs = [ git git-lfs nodejs ];
    GIT_SSL_NO_VERIFY = "true";
    preBuild = "export HOME=$(mktemp -d)";
}
