- Start Date: 2021-12-02
- Status: Proposed

# Run interactive sessions

## Summary

Propose a new sub-group command `renku session` that is responsible for
managing interactive sessions, both local and remote, for a project.

## Motivation

Renkulab.io provides the option to run interactive sessions of a project. Among other things the session
contains a jupyter notebook.

The motivation behind this RFC is to provide a simple renku CLI sub-command that basically allows the
starting and stopping of these interactive sessions.

## Design Detail

The proposed sub-commands of `renku session`, namely to run interactive sessions. The local session heavily relies on
[docker](https://www.docker.com/). Docker is one of the most popular container engines, that eases the development
and deployment of applications, by off-loading the installation of the runtime environment dependencies of the
application.

In order to start a local interactive session, just as in case of renkulab.io, it is required to provide
a `Dockerfile`. This is by default provided by `renku init` when creating a renku project, but if the user
is using a custom project template it might not be present.

### Proposed Commands

#### renku session start

A command to start an interactive session.

By default, the `Dockerfile` present in the given project is going to be used for building the docker image
that's going to be used for running the interactive session. In case the docker image is not available the
user is going to be prompted whether the user wants to build the docker image, as in some cases it can be
time consuming.

With the `--provider <provider_type>` flag a user can specify session provider to be used for starting
an interactive session. By default two different providers are shipped with Renku CLI:
 - docker
 - renkulab
The provider specific configuration values can be specified by using the `--config <config.yaml>` flag.

##### Detailed Parameter Description

```
renku session start [OPTIONS]

--image <image_name>			Override the docker image to be used for the interactive session.
-p|--provider [docker|renkulab] 	Use the specified backend for starting an interactive session.
-c|--config   				YAML file containing configuration for the provider.
--cpu <num of cpus>			Specify how much of the available CPU resources a container can use.
--disk <disk space>			Specify the amount of disk space for the container.
--gpu <device>				Specify the GPU allocated for the container.
--memory <max memory>			The maximum amount of memory the container can use.

```

#### renku session stop

`renku session stop my_session` simply stops the given interactive session.

One can shut down all the currently running interactive sessions at once with the `renku session stop --all`.

##### Detailed Parameter Description

```
renku session stop [OPTIONS] <name>

<name>      Name of the session that the user wants to stop.

--all       Stops all the running containers.

```

#### renku session list

The command to list all the currently running interactive sessions for the given project, with the
detailed information regarding the network port mappings.

##### Detailed Parameter Description

```
renku session list [OPTIONS]

-p|--provider [docker|renkulab] 	Use the specified backend for listing the interactive sessions.
-c|--config   				YAML file containing configuration for the provider.

```

#### renku session open

`renku session open <my_session>` opens the given interactive session in the user's default browser.

Note, it is possible that the user is running renku on a remote machine via SSH. In this case this command
might fail, unless for example X-Fowarding is not enabled.

## Drawbacks

One of the major drawback of the current design is the dependency on docker. Namely, to only support local
interactive session if docker is available. Many HPC environments do not support docker, but they support
other container formats, like [runC](https://github.com/opencontainers/runc). As a long term goal of this
effort it would be good to consider support for different container engines, hence the design and implementation
of the initial `renku session` should take this into consideration.

## Rationale and Alternatives

> Why is this design the best in the space of possible designs?

> What other designs have been considered and what is the rationale for not choosing them?

> What is the impact of not doing this?

## Unresolved questions

> What parts of the design do you expect to resolve through the RFC process before this gets merged?

> What parts of the design do you expect to resolve through the implementation of this feature before stabilisation?

> What related issues do you consider out of scope for this RFC that could be addressed in the future independently of the solution that comes out of this RFC?
