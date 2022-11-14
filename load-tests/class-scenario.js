import http from "k6/http";
import exec from "k6/execution";
import { Trend } from "k6/metrics";

import { renkuLogin } from "./oauth.js";
import { check, fail, sleep } from "k6";

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
      vus: 10, // tested up to 30
      iterations: 1,
    },
  },
};

// k6 custom metrics
const sessionStartupTrend = new Trend("session_startup");

function httpRetry(httpRequest, n, logMessage) {
  let res,
    i = 0;
  do {
    sleep(i);
    res = httpRequest;
    console.log(
      `${exec.vu.idInInstance}-vu: ${logMessage}, status: ${res.status}, retries: ${i}`
    );
    i++;
  } while (!(res.status >= 200 && res.status < 300) && i < n);

  if (res.status >= 400) {
    throw new Error(
      `${exec.vu.idInInstance}-vu: FAILED ${logMessage}, status: ${res.status}, retry: ${i}`
    );
  }

  return res;
}

function showProjectInfo(baseUrl, gitUrl) {
  const payload = {
    git_url: gitUrl,
    is_delayed: false,
    migrate_project: false,
  };
  const res = http.post(
    `${baseUrl}/ui-server/api/renku/project.show`,
    JSON.stringify(payload),
    { headers: { "Content-Type": "application/json" } }
  );
  console.log(res.status);
  if (
    !check(res, {
      "getting project info succeeded with 2XX": (res) =>
        res.status >= 200 && res.status < 300,
      "getting project info response has no error": (res) =>
        res.json().error === undefined,
    })
  ) {
    fail(
      `getting project info failed with status ${res.status} and body ${res.body}`
    );
  }

  return JSON.parse(res.body);
}

function forkProject(baseUrl, projectInfo) {
  const name = projectInfo.result.name;
  const projectPathComponents = projectInfo.result.id.split("/");
  const path = projectPathComponents.pop();
  const namespace_path = projectPathComponents.pop();
  const id = namespace_path + "%2F" + path;

  const vuIdPostfix = "-" + String(exec.vu.idInInstance);

  console.log(`${exec.vu.idInInstance}-vu: project id: ${id}`);

  const payload = {
    id: id,
    name: name + vuIdPostfix,
    namespace_path: namespace_path,
    path: path + vuIdPostfix,
  };

  const res = httpRetry(
    http.post(
      `${baseUrl}/ui-server/api/projects/${id}/fork`,
      JSON.stringify(payload),
      { headers: { "Content-Type": "application/json" } }
    ),
    10,
    "fork project"
  );

  return JSON.parse(res.body);
}

function getCommitShas(baseUrl, projectInfo) {
  const id = projectInfo.id;
  console.log(`${exec.vu.idInInstance}-vu: project id to fork ${id}`);

  const res = httpRetry(
    http.get(
      `${baseUrl}/ui-server/api/projects/${id}/repository/commits?ref_name=master&per_page=100&page=1`
    ),
    10,
    "get commit sha"
  );

  //console.log(`${exec.vu.idInInstance}-vu: commit sha request status: ${res.status}`)

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

  const res = httpRetry(
    http.post(
      `${baseUrl}/ui-server/api/notebooks/servers`,
      JSON.stringify(payload),
      { headers: { "Content-Type": "application/json" } }
    ),
    10,
    "start server/session"
  );

  console.log(
    `${exec.vu.idInInstance}-vu: start server, status: ${res.status}`
  );

  return JSON.parse(res.body);
}

function pollServerStatus(baseUrl, server) {
  const serverName = server.name;
  console.log(`${exec.vu.idInInstance}-vu: server name: ${serverName}`);

  const ServerStates = {
    Starting: "starting",
    Running: "running",
  };

  let resBody,
    counter = 0;
  do {
    sleep(1);
    resBody = JSON.parse(
      http.get(`${baseUrl}/ui-server/api/notebooks/servers/${serverName}`).body
    );
    counter++;
  } while (
    resBody.status === undefined ||
    resBody.status.state == ServerStates.Starting
  );

  sessionStartupTrend.add(counter);

  return resBody;
}

function stopServer(baseUrl, server) {
  const serverName = server.name;
  const res = http.del(
    `${baseUrl}/ui-server/api/notebooks/servers/${serverName}`
  );

  return res.status;
}

function deleteProject(baseUrl, projectInfo) {
  const id = projectInfo.id;

  const res = httpRetry(
    http.del(`${baseUrl}/ui-server/api/projects/${id}`),
    10,
    "delete project"
  );

  console.log("shuttdown");

  return res.status;
}

// Test

export function setup() {
  renkuLogin(baseUrl, credentials);

  const projectInfo = showProjectInfo(baseUrl, sampleGitProjectUrl);

  return projectInfo;
}

export default function test(projectInfo) {
  const vu = exec.vu.idInInstance;

  sleep(vu); // lets VUs start in sequence

  console.log(`${vu}-vu: login to renku`);
  renkuLogin(baseUrl, credentials);

  console.log(`${vu}-vu: fork 'test' project -> 'test-${vu}'`);
  const forkedProject = forkProject(baseUrl, projectInfo);

  sleep(90); // workaround

  console.log(`${vu}-vu: get latest commit hash from forked project`);
  const commitShas = getCommitShas(baseUrl, forkedProject);

  console.log(`${vu}-vu: start server/session with latest commit`);
  const server = startServer(baseUrl, forkedProject, commitShas);

  console.log(`${vu}-vu: wait for server to enter state 'running'`);
  pollServerStatus(baseUrl, server);
  console.log(`${vu}-vu: server 'running'`);

  console.log(`${vu}-vu: let server run for 200 seconds`);
  sleep(200);

  console.log(`${vu}-vu: shutdown server`);
  stopServer(baseUrl, server);

  console.log(`${vu}-vu: delete 'project-${vu}'`);
  deleteProject(baseUrl, forkedProject);

  console.log(`${vu}-vu: test finished`);
}
