- Start Date: 2021-12-02
- Status: Proposed

# Run interactive sessions

## Summary

Propose a new sub-group command `renku session` that is responsible for
managing interactive sessions, both local and remote, for a project.

## Motivation

A renkulab instance (such as Renkulab.io) provides the option to run interactive sessions of a project. Among other things the session
contains a jupyter server.

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

The resource related options of the command (`--cpu`, `--memory` etc.) will override the default values
of the provider as well as the [project level resource settings](https://renku.readthedocs.io/en/latest/reference/templates.html#renku).

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

<name>      				Name of the session that the user wants to stop.

--all       				Stops all the running containers.
-p|--provider [docker|renkulab] 	Use the specified backend for starting an interactive session.

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

##### Detailed Parameter Description

```
renku session open <name>

<name>      				Name of the session that the user wants to stop.

-p|--provider [docker|renkulab] 	Use the specified backend for listing the interactive sessions.

```

## Drawbacks

One of the major drawback of the current design is the dependency on docker. Namely, to only support local
interactive session if docker is available. Many HPC environments do not support docker, but they support
other container formats, like [runC](https://github.com/opencontainers/runc). As a long term goal of this
effort, support for different container engines is essential. In order to allow extending the `renku session`
command with different container engines a very simple API is defined.

The interface requires that the container engine support the following functionalities:
 - finding a specific container image. Both locally (cached) and as well in a remote repository.
 - building a container image based on an image specs file.
 - starting a container using a specific image.
 - stopping a running container.
 - get the URL of the interactive session of a running container.

The implementation of this interface in detail:

```python
class ISessionProvider:

    def build_image(self, image_descriptor: Path, image_name: str, config: Optional[Dict[str, Any]]) -> Optional[str]:
        """Builds the container image.
        :param image_descriptor: Path to the container image descriptor file.
        :param image_name: Container image name.
        :param config: Path to the session provider specific configuration YAML.
        :returns: a unique id for the created interactive sesssion.
        """
        pass

    def find_image(self, image_name: str, config: Optional[Dict[str, Any]]) -> bool:
        """Search for the given container image.
        :param image_name: Container image name.
        :param config: Path to the session provider specific configuration YAML.
        :returns: True if the given container images is available locally.
        """
        pass

    def session_provider(self) -> Tuple["ISessionProvider", str]:
        """Supported session provider.
        :returns: a tuple of ``self`` and engine type name.
        """
        pass

    def session_list(self, project_name: str, config: Optional[Dict[str, Any]]) -> List[Session]:
        """Lists all the sessions currently running by the given session provider.
        :param project_name: Renku project name.
        :param config: Path to the session provider specific configuration YAML.
        :returns: a list of sessions.
        """
        pass

    def session_start(
        self,
        image_name: str,
        project_name: str,
        config: Optional[Dict[str, Any]],
        client: LocalClient,
        cpu_request: Optional[float] = None,
        mem_request: Optional[str] = None,
        disk_request: Optional[str] = None,
        gpu_request: Optional[str] = None,
    ) -> str:
        """Creates an interactive session.
        :param image_name: Container image name to be used for the interactive session.
        :param project_name: The project identifier.
        :param config: Path to the session provider specific configuration YAML.
        :param client: Renku client.
        :param cpu_request: CPU request for the session.
        :param mem_request: Memory size request for the session.
        :param disk_request: Disk size request for the session.
        :param gpu_request: GPU device request for the session.
        :returns: a unique id for the created interactive sesssion.
        """
        pass

    def session_stop(self, project_name: str, session_name: Optional[str], stop_all: bool) -> bool:
        """Stops all or a given interactive session.
        :param client: Renku client.
        :param session_name: The unique id of the interactive session.
        :param stop_all: Specifies whether or not to stop all the running interactive sessions.
        :returns: True in case session(s) has been successfully stopped
        """
        pass

    def session_url(self, session_name: str) -> str:
        """Get the given session's URL.
        :param session_name: The unique id of the interactive session.
        :returns: URL of the interactive session.
        """
        pass
```
