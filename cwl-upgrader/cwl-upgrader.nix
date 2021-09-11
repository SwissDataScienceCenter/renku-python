let
    pkgs = import <nixpkgs> { };

    mach-nix = import (builtins.fetchGit {
        url = "https://github.com/uwmisl/mach-nix";
        ref = "master";
    }) {
      inherit pkgs;
      pypiDataRev = "5c6e5ecbc5a60fb9c43dc77be8e0eb8ac89f4fee";
      pypiDataSha256 = "0gnq6r92bmnhqjykx3jff7lvg7wbpayd0wvb0drra8r8dvmr5b2d";
    };
    ruamel = mach-nix.mkPython {
        requirements = ''
            ruamel.yaml<0.16.5"
            schema-salad
            rdflib<6.0
        '';
    };

in  with pkgs;
    python39.pkgs.buildPythonPackage {
        pname = "cwl-upgrader";
        version = "1.12";

        src = fetchFromGitHub {
            owner = "common-workflow-language";
            repo = "cwl-upgrader";
            rev = "e3f995270cd6c52e8e2e1ad0b02a6c92d7333bea";
            sha256 = "1hhsbz0326dwrjpvxf654vqg4id6402fbvgpp5bkw1d2l2x9sr83";
            leaveDotGit = true;
        };
        doCheck = false;
        patches = [ ./cwl-upgrader-typing.patch ];
        propagatedBuildInputs = [ ruamel ];
    }
