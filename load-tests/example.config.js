export const baseUrl = "https://dev.renku.ch";
// oldGitlabProjectId has to point to a project that resides in a namespace that the user
// has at least maintainer access to. This is because the load tests will fork this project
// into the same namespace as where the original project resides and only generate a uuid-like
// name for the project. So if you point to a project that resides in a namespace to which
// the test runner has no permissions, the forking part of the tests will fail.
export const oldGitlabProjectId = 5011;
// This project is used to test calling api/renku/project.show, the project is not forked
// and it does not have the same strict requirements as the project mentioned above. Any
// public project should work here (whether the user has write access to it or not).
export const sampleGitProjectUrl =
  "https://dev.renku.ch/gitlab/tasko.olevski/test-project-2.git";

// Two sets of credentials are needed only if the Renku deployment
// has a separate Gitlab that requires logging into another Renku
// instance. So for dev.renku.ch you need one set of credentials
// for CI deployments you need 2. First the credentials to the
// CI deployment then the ones for dev.renku.ch.
export const credentials = [
  {
    username: "user@email.com",
    password: "secret-password1",
  },
  {
    username: "user@email.com",
    password: "secret-password1",
  },
];
