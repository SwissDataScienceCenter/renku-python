workflow "Publish workflow" {
  on = "push"
  resolves = [
    "Docker Tag",
  ]
}

action "Shellcheck for GitHub Actions" {
  uses = "actions/bin/shellcheck@1b3c130914f7b20890bf159306137d994a4c39d0"
}

action "GitHub Action for Docker" {
  uses = "actions/docker/cli@c08a5fc9e0286844156fefff2c141072048141f6"
  needs = ["Shellcheck for GitHub Actions"]
  args = "build -t renku-python ."
}

action "Docker Tag" {
  uses = "actions/docker/tag@c08a5fc9e0286844156fefff2c141072048141f6"
  needs = ["GitHub Action for Docker"]  
  args = "renku-python renku/renku-python"
}

action "Docker Registry" {
  uses = "actions/docker/login@c08a5fc9e0286844156fefff2c141072048141f6"
  needs = ["Docker Tag"]
  secrets = ["DOCKER_USERNAME", "DOCKER_PASSWORD"]
}
