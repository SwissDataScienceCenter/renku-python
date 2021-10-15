# Renku Core Service API Versioning

To maintain for backwards compatibility and not force migrations on users, we need to support multiple versions of the core-service in parallel.

This boils down to two concerns, supporting multiple metadata versions for the metadata in the repository, and supporting multiple versions
of the core service API.

## Multiple Metadata Versions

This section is about how to deal with changes in how renku stores its internal metadata (in `.renku/`).

### Problem
We sometimes have to change how we store metadata internally in renku. This can be big changes like switching from the *.yml based approach of storing metadata
to the database based approach that is being introduced in 1.0.0, or minor changes like adding new fields to entities.

Due to such changes, older projects without these changes aren't supported with the current version and require a migration to be supported.
Usually it is not possible to support multiple metadata versions in the same version of renku, as this would bloat the code, make it hard to maintain and
has a high potential of introducing additional bugs.

Because of this, it makes more sense to deploy new and old versions of the core service side by side, and let the UI pick which one to use for each project.

### Detecting if a project is supported

`/cache.migrations_check` only actually needs `.renku/metadata/root` and `.renku/metadata/project` checked out to work, plus optionally `Dockerfile` to return dockerfile info.

We don't want to clone the repo as that might be slow on big projects, so we could just go with the approach outlined here https://stackoverflow.com/a/64776113/212971 and clone those files to a temporary folder, and return the migrations_check on that and that would return the normal migrations check. We would need to do this twice, once with (".renku/metadata/root", ".renku/metadata/project", "Dockerfile") and if that fails (old project) then with (".renku/metadata.yml", "Dockerfile").

This should be faster than cloning the repo, though still slow from a UX perspective (one call takes ~0.7s on my machine).

Then if the migrations check says project is out of date, the UI can try the earlier core service.

### Service URLs/UX

The URL for the request is something like https://renkulab.io/api/renku/cache.migrations_check. We could deploy an earlier version at something like https://renkulab.io/api/previous/renku/cache.migrations_check and the UI remembers if a project failed the check and then defaults to previous. And maybe have the current one at https://renkulab.io/api/current/renku/cache.migrations_check ?

If a user clicks the "migrate" button, this has to be sent to the current version of the service (important if UI is defaulting to `previous`) and after it has to default to current again.

An important thing to note is that all this is only necessary for changes to our metadata, not for version changes. e.g. if we have (0.16.2, 1.0.0) deployed, and we make a bugfix release 1.0.1 that does not change the metdata model, we want to make a renku release that has (0.16.2,1.0.1), NOT (1.0.0, 1.0.1). We might be able to automate this in some smart way, since we have the metadata version (currently `9`) stored, and we know if this changes. Doing it manually also works.

#### Fancier alternative
If we could merge https://github.com/SwissDataScienceCenter/renku-python/pull/2122 we could go for a much smarter approach, since there we actually get back the metadata version, e.g. `9`, not renku version like `0.16.2`. In this case, the initial /cache.migrations_check request tells you if the repo is not supported and exactly what metadata version the project is on.
Then we could have as many core services deployed as we want, e.g. `https://renkulab.io/api/renku/` for the current one, `https://renkulab.io/api/v8/renku/` for metadata version 8, `https://renkulab.io/api/v7/renku/` for 7 and so on. And the UI could directly go to the correct one based on the initial `check_migration` request.


### Caveats
Some things won't work on older versions of the core service, namely:
- Dataset import from a repo that's on a newer version
- Migrate project (obviously)

## Core service API versioning

### Problem
There are two other issues with running multiple versions of core-service in parallel, namely:
- New endpoints that don't exist
- Existing endpoints that have a modified interface

For new endpoints that don't exist, well, we won't backport them to old versions, that's too much effort. The question there is, how does the UI know what is and isn't supported? Similarly, for existing endpoints where the interface changes, we can try and deprecate things but keep them functional in case of deleted parameters, but if we add parameters, the service also raises an exception if it gets a parameter that it doesn't know.
So the UI couldn't just call old endpoints and expect everything to work.


### API Versioning
We could handle this manually but I'd bet money this will lead to errors at some point. Plus we wouldn't want to just have the UI figure out how to call different versions of core service.

We would need to modify the `/version` endpoint to return the current API version of the service. A big question is whether a single version of core-service
should support multiple API versions. In some cases this is trivial (e.g. when adding a non-mandatory field like images on dataset), sometimes harder (e.g.
the switch from returning slug&name for project instead of returning slug as `name` ) to very hard (e.g. new migrations_check endpoint where the underlying code
to calculate migrations status was completely reworked).

It is also not clear in how much we should rely on UI dealing with these things. Having code like "if core-service-version==v1 then send_v1_request() else send_v2_request()"
does introduce additional overhead and a balance should be struck between continuing support for old project and developing new features for user that keep up to date.
If we do go with an approach like this, the question becomes how to handle the versioning, be it with separate URLs like /renku/v1/datasets.list, with query parameters
like /renku/datasets.list?version=1 or with headers like "Accept: version=1.0".

An alternative would be to selectively phase out support. E.g. if we change something fundamental about dataset.add_file, then users can't add files to datasets anymore
through the UI unless they migrate, but other endpoints still work. This results in less overhead for us, but at the cost of only partial support for old projects in some cases.