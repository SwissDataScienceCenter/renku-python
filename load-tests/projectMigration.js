import http from "k6/http";
import { check, fail, sleep } from "k6";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.2.0/index.js";

import { renkuLogin } from "./oauth.js";
import { credentials, baseUrl, oldGitlabProjectId } from "./config.js";

export const options = {
  scenarios: {
    testUploads: {
      executor: "per-vu-iterations",
      vus: 3,
      iterations: 1,
    },
  },
};

function forkProject(baseUrl, gitlabProjectId) {
  const uuid = uuidv4();
  let projectData = http.get(
    `${baseUrl}/ui-server/api/projects/${gitlabProjectId}`
  );
  check(projectData, {
    "response code for getting project info was 2XX": (res) =>
      res.status >= 200 && res.status < 300,
  });
  projectData = projectData.json();
  let forkName = `test-forked-project-${uuid}`;
  const payload = {
    id: gitlabProjectId,
    name: forkName,
    namespace_id: projectData.namespace.id,
    path: forkName,
    visibility: projectData.visibility,
  };
  let forkRes = http.post(
    `${baseUrl}/ui-server/api/projects/${gitlabProjectId}/fork`,
    payload
  );
  if (
    !check(forkRes, {
      "response code for forking was 2XX": (res) =>
        res.status >= 200 && res.status < 300,
    })
  ) {
    fail(`forking failed with code ${forkRes.status} and body ${forkRes.body}`);
  }
  sleep(5);
  return forkRes;
}

function migrateProject(baseUrl, gitlabProjectId) {
  let projectData = http.get(
    `${baseUrl}/ui-server/api/projects/${gitlabProjectId}`
  );
  check(projectData, {
    "response code for getting project info is 2XX": (res) =>
      res.status >= 200 && res.status < 300,
  });
  projectData = projectData.json();
  const migratePayload = {
    branch: projectData.default_branch,
    force_template_update: true,
    git_url: projectData.http_url_to_repo,
    is_delayed: false,
    skip_docker_update: false,
    skip_migrations: false,
    skip_template_update: false,
  };
  const migrateRes = http.post(
    `${baseUrl}/ui-server/api/renku/cache.migrate`,
    JSON.stringify(migratePayload),
    { headers: { "Content-Type": "application/json" } }
  );
  if (
    !check(migrateRes, {
      "response code for migrating is 2XX": (res) =>
        res.status >= 200 && res.status < 300,
    })
  ) {
    fail(`migration completed with code ${migrateRes.status}`);
  }
  if (
    !check(migrateRes, {
      "migration request has no errors": (res) =>
        res.json().error === undefined,
    })
  ) {
    fail(
      `migration completed with errors: ${JSON.stringify(
        migrateRes.json().error
      )}`
    );
  }
  check(migrateRes, {
    "was_migrated is true in migration response": (res) =>
      res.json().result.was_migrated,
  });
  return migrateRes;
}

export default function test() {
  renkuLogin(baseUrl, credentials);
  const forkedProjectResponse = forkProject(baseUrl, oldGitlabProjectId);
  const forkProjectId = forkedProjectResponse.json().id;
  migrateProject(baseUrl, forkProjectId);
  const res = http.del(`${baseUrl}/ui-server/api/projects/${forkProjectId}`);
  check(res, {
    "deletion of fork succeeded with 2XX": (res) =>
      res.status >= 200 && res.status < 300,
  });
}
