import http from "k6/http";
import exec from "k6/execution";
import { Counter, Trend } from "k6/metrics";

import { renkuLogin } from "./oauth.js";
import { check, fail, group, sleep } from "k6";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.2.0/index.js";

import {
  baseUrl,
  credentials,
  sampleGitProjectUrl,
  serverOptions,
} from "./config.js";

export const options = {
  scenarios: {
    lecture: {
      executor: "per-vu-iterations",
      vus: 4, // tested up to 30
      iterations: 1,
      maxDuration: '30m',
      gracefulStop: '2m',
    },
  },
};

// k6 custom metrics
const sessionStartupDuration = new Trend("session_startup_duration", true);
const sessionCreateReqDuration = new Trend("session_create_req_duration", true);
const sessionGetReqDuration = new Trend("session_get_req_duration", true);
const sessionDeleteReqDuration = new Trend("session_delete_req_duration", true);
const requestRetries = new Counter("http_request_retries", false);

function DoHttpRequest(httpRequest, nRetries = 0) {
  let res,
    i = 0;
  while (i <= nRetries) {
    res = httpRequest();
    if (res.status >= 400 || res.status < 200) {
      i++;
      requestRetries.add(1)
      sleep(i);
      continue;
    }
    break;
  };

  generalResponseCheck(res);
  return res;
}

function generalResponseCheck(res) {
  if (
    !check(res, {
      "request succeeded with 2XX": (res) => res.status >= 200 && res.status < 300,
    })
  ) {
    fail(
      `request at ${res.url} failed with ${res.status} and body ${res.body}`
    );
  }
}

function showProjectInfo(baseUrl, gitUrl) {
  const payload = {
    git_url: gitUrl,
    is_delayed: false,
    migrate_project: false,
  };
  const res = DoHttpRequest(
    () => http.post(
      `${baseUrl}/ui-server/api/renku/project.show`,
      JSON.stringify(payload),
      { headers: { "Content-Type": "application/json" } }
    )
  )
  if (
    !check(res, {
      "getting project info response has no error": (res) =>
        res.json().error === undefined,
    })
  ) {
    fail(
      `getting project info failed with error ${res.json().error}`
    );
  }

  return res.json();
}

function forkProject(baseUrl, projectInfo, idPostfix) {
  const name = projectInfo.result.name;
  const projectPathComponents = projectInfo.result.id.split("/");
  const path = projectPathComponents.pop();
  const namespace_path = projectPathComponents.pop();
  const id = namespace_path + "%2F" + path;

  const payload = {
    id: id,
    name: name + idPostfix,
    namespace_path: namespace_path,
    path: path + idPostfix,
  };

  const res = DoHttpRequest(
    () => http.post(
      `${baseUrl}/ui-server/api/projects/${id}/fork`,
      JSON.stringify(payload),
      { headers: { "Content-Type": "application/json" } }
    ),
    10,
  );

  return res.json();
}

function getCommitShas(baseUrl, projectInfo) {
  const id = projectInfo.id;

  const res = DoHttpRequest(
    () => http.get(
      `${baseUrl}/ui-server/api/projects/${id}/repository/commits?ref_name=master&per_page=100&page=1`
    ),
    10,
  );

  return JSON.parse(res.body);
}

function startServer(baseUrl, forkedProject, commitShas) {
  const payload = {
    branch: "master",
    commit_sha: commitShas[0].id,
    namespace: forkedProject.namespace.path,
    project: forkedProject.name,
    serverOptions: serverOptions,
  };

  const res = DoHttpRequest(
    () => http.post(
      `${baseUrl}/ui-server/api/notebooks/servers`,
      JSON.stringify(payload),
      { headers: { "Content-Type": "application/json" } }
    ),
    1,
  );
  sessionCreateReqDuration.add(res.timings.duration)
  return res.json();
}

function pollServerStatus(baseUrl, server) {
  const serverName = server.name;

  const ServerStates = {
    Starting: "starting",
    Running: "running",
  };

  let resJson, res, counter = 0;
  do {
    sleep(1);
    res = DoHttpRequest(
      () => http.get(`${baseUrl}/ui-server/api/notebooks/servers/${serverName}`)
    )
    sessionGetReqDuration.add(res.timings.duration)
    resJson = res.json()
    counter++;
  } while (
    resJson.status === undefined ||
    resJson.status.state == ServerStates.Starting
  );

  sessionStartupDuration.add(counter * 1000);

  return resJson;
}

function stopServer(baseUrl, server) {
  const serverName = server.name;
  const res = DoHttpRequest(
    () => http.del(
      `${baseUrl}/ui-server/api/notebooks/servers/${serverName}`
    ),
  )
  sessionDeleteReqDuration.add(res.timings.duration)

  return res.status;
}

function deleteProject(baseUrl, projectInfo) {
  const id = projectInfo.id;

  const res = DoHttpRequest(
    () => http.del(`${baseUrl}/ui-server/api/projects/${id}`),
    10,
  );

  return res.status;
}

// Test setup
export function setup() {
  renkuLogin(baseUrl, credentials);
  const projectInfo = showProjectInfo(baseUrl, sampleGitProjectUrl);
  return projectInfo;
}

// Test code
export default function test(projectInfo) {
  let forkedProject, commitShas, server;
  const uuid = uuidv4();

  renkuLogin(baseUrl, credentials);

  group("fork", function () {
    forkedProject = forkProject(baseUrl, projectInfo, uuid);
    sleep(90); // waiting for fork to complete
    commitShas = getCommitShas(baseUrl, forkedProject);
  })

  group("launch session", function () {
    server = startServer(baseUrl, forkedProject, commitShas);
    pollServerStatus(baseUrl, server);
  })

  sleep(10); // simulate users being idle

  group("shutdown server", function () {
    stopServer(baseUrl, server);
  })

  group("remove project", function () {
    deleteProject(baseUrl, forkedProject);
  })
}
