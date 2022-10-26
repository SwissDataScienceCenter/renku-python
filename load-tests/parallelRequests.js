import http from 'k6/http';
import { check, fail, sleep } from 'k6';

import { renkuLogin } from './oauth.js'
import { credentials, baseUrl, sampleGitProjectUrl } from './config.js'

export const options = {
  scenarios: {
    testUploads: {
      executor: 'per-vu-iterations',
      vus: 120,
      iterations: 1,
    },
  }
}

function getTemplates(baseUrl, templatesRef) {
  const res = http.get(`${baseUrl}/ui-server/api/renku/templates.read_manifest?url=https%3A%2F%2Fgithub.com%2FSwissDataScienceCenter%2Frenku-project-template&ref=${templatesRef}`)
  if (!check(res, {
    "reading templates succeeded with 2XX": (res) => res.status >= 200 && res.status < 300,
    "reading templates response has no error": (res) => res.json().error === undefined,
    "reading templates response more than zero templates": (res) => res.json().result.templates.length > 0,
  })) {
    fail(`reading templates failed with status code ${res.status} and response ${res.body}`)
  }
  return res
}

function showProjectInfo(baseUrl, gitUrl) {
  const payload = {
    git_url: gitUrl,
    is_delayed: false,
    migrate_project: false
  }
  const res = http.post(
    `${baseUrl}/ui-server/api/renku/project.show`,
    JSON.stringify(payload),
    { headers: { "Content-Type": "application/json" } },
  )
  if (!check(res, {
    "getting project info succeeded with 2XX": (res) => res.status >= 200 && res.status < 300,
    "getting project info response has no error": (res) => res.json().error === undefined,
  })) {
    fail(`getting project info failed with status ${res.status} and body ${res.body}`)
  }
  return res
}

export default function test() {
  renkuLogin(baseUrl, credentials)
  const templatesRef = "0.3.4"
  getTemplates(baseUrl, templatesRef)
  sleep(2)
  showProjectInfo(baseUrl, sampleGitProjectUrl)
}
