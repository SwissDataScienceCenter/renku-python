// Creator: k6 Browser Recorder 0.6.2

import { sleep, check, fail } from "k6";
import http from "k6/http";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.2.0/index.js";
import crypto from "k6/crypto";
import { URL } from "https://jslib.k6.io/url/1.0.0/index.js";

import { renkuLogin } from "./oauth.js";
import { credentials, baseUrl } from "./config.js";

export const options = {
  scenarios: {
    testUploads: {
      executor: "per-vu-iterations",
      vus: 3,
      iterations: 1,
    },
  },
};

function uploadRandomFile(baseUrl, uuid, fileName, numChunks, chunkSizeBytes) {
  const responses = [];
  for (let i = 0; i < numChunks; i++) {
    const url = new URL(`ui-server/api/renku/cache.files_upload`, baseUrl);
    url.searchParams.append("dzuuid", uuid);
    url.searchParams.append("dzchunkindex", i);
    url.searchParams.append("dztotalfilesize", numChunks * chunkSizeBytes);
    url.searchParams.append("dzchunksize", chunkSizeBytes);
    url.searchParams.append("dztotalchunkcount", numChunks);
    url.searchParams.append("dzchunkbyteoffset", i * chunkSizeBytes);
    url.searchParams.append("chunked_content_type", "application/octet-stream");
    const res = http.post(url.toString(), {
      file: http.file(
        crypto.randomBytes(chunkSizeBytes),
        fileName,
        "application/octet-stream"
      ),
    });
    responses.push(res);
  }
  if (
    !check(responses, {
      "file uploads all have 200 repsonses": (responses) =>
        responses.every((res) => res.status === 200),
      "file uploads all completed without errors": (responses) =>
        responses.every((res) => res.json().error === undefined),
    })
  ) {
    const errResponses = responses
      .filter((res) => res.json().error !== undefined)
      .map((res) => res.json());
    const failedResponsesBody = responses
      .filter((res) => res.status != 200)
      .map((res) => res.body);
    const failedResponsesCodes = responses
      .filter((res) => res.status != 200)
      .map((res) => res.status);
    fail(
      `some responses failed with errors ${JSON.stringify(
        errResponses
      )}\nsome respones ` +
        `failed with non-200 status codes codes: ${JSON.stringify(
          failedResponsesCodes
        )} bodies: ${JSON.stringify(failedResponsesBody)}`
    );
  }
  return responses[numChunks - 1];
}

export default function fileUpload() {
  renkuLogin(baseUrl, credentials);
  const baseUrlResponse = http.get(baseUrl);
  const projects = http.get(
    `${baseUrl}/ui-server/api/projects?query=last_activity_at&per_page=100&starred=true&page=1`
  );
  check(baseUrlResponse, {
    "baseUrl responds with status 200": (r) => r.status === 200,
  });
  check(projects, {
    "projects list endpoint responds with status 200": (r) => r.status === 200,
    "project list exists": (r) => r.json().length >= 0,
  });
  sleep(1);
  const uploads = http.get(`${baseUrl}/ui-server/api/renku/cache.files_list`);
  check(uploads, {
    "uploads list response does not have errors": (r) =>
      r.json().error === undefined,
    "uploads list response contains a list of uploads": (r) =>
      r.json().result.files.length >= 0,
  });
  sleep(1);
  const uuid = uuidv4();
  const fileName = `${uuid}.bin`;
  const fileUploadResponse = uploadRandomFile(
    baseUrl,
    uuid,
    fileName,
    100,
    1e6
  );
  let uploadedFiles = fileUploadResponse.json().result.files;
  if (uploadedFiles === undefined) {
    uploadedFiles = [];
  }
  uploadedFiles = uploadedFiles.map((i) => i.file_name);
  if (
    !check(uploadedFiles, {
      "file name found in last upload response": (r) => r.includes(fileName),
    })
  ) {
    fail(
      `could not find file in last upload response, body: ${fileUploadResponse.body}, status code: ${fileUploadResponse.status}`
    );
  }
}
