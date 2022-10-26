export const baseUrl = "https://dev.renku.ch"
export const oldGitlabProjectId = 5011
export const sampleGitProjectUrl = "https://dev.renku.ch/gitlab/tasko.olevski/test-project-2.git"

// Two sets of credentials are needed only if the Renku deployment
// has a separate Gitlab that requires logging into another Renku
// instance. So for dev.renku.ch you need one set of credentials
// for CI deployments you need 2. First the credentials to the
// CI deployment then the ones for dev.renku.ch.
export const credentials = [
  {
    username: "user@email.com",
    password: "secret-password1"
  },
  {
    username: "user@email.com",
    password: "secret-password1"
  },
]
