import http from 'k6/http';
import { parseHTML } from 'k6/html';

function handleRenkuLoginForm(httpResponse, username, password) {
  const doc = parseHTML(httpResponse.body);
  const actionUrl = doc.find('#kc-form-login').attr('action');
  if (!actionUrl) {
    throw new Error(`Could not locate login form in http response ${httpResponse.body}`)
  }
  const loginData = {
    username,
    password,
    credentialId: '',
  };
  return http.post(actionUrl, loginData)
}

function followRedirectLinkFromHtml(httpResponse) {
  const doc = parseHTML(httpResponse.body);
  let url = doc.find('a').attr("href")
  if (!url) {
    throw new Error(`Could not find <a> element with href attribute in ${httpResponse.body}`)
  }
  if (url.endsWith("/")) {
    // leaving trailing slashes here results in 404
    url = url.slice(0,-1)
  }
  return http.get(url)
}

export function renkuLogin(baseUrl, credentials) {
  // double slashes when composing url causes trouble and 404s
  if (baseUrl.endsWith("/")) {
    baseUrl = baseUrl.slice(0,-1)
  }
  // the trailing slash is needed here keycloak accepts only such and longer callbacks
  const redirectUrl = `${baseUrl}/`
  let finalResponse = null
  const res1 = http.get(`${baseUrl}/ui-server/auth/login?redirect_url=${redirectUrl}"`)
  const res2 = handleRenkuLoginForm(res1, credentials[0].username, credentials[0].password)
  const res3 = followRedirectLinkFromHtml(res2)
  if (res3.body.match(".*redirect.*|.*Redirect.*") && parseHTML(res3.body).find("a").toArray().length > 0) {
    // no more login forms just follow a single last redirect
    finalResponse = followRedirectLinkFromHtml(res3)
  }
  else if (parseHTML(res3.body).find('#kc-form-login').toArray().length > 0) {
    // one more login required, usually happens for ci and similar deployments that do not have their own gitlab
    const res4 = handleRenkuLoginForm(res3, credentials[1].username, credentials[1].password)
    finalResponse = followRedirectLinkFromHtml(res4)
  }
  if (finalResponse.status != 200) {
    throw new Error(`Could not successfully login, expected status code 200 but got ${finalResponse.status}`)
  }
}
