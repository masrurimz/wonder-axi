import { Data, Option } from "effect";

import { MCP_URL } from "./mcp.ts";

export const AUTH_BASE = "https://oauth.wonder.so";

export const REGISTER_URL = `${AUTH_BASE}/oauth2/register`;

export const AUTHORIZE_URL = `${AUTH_BASE}/oauth2/authorize`;

export const TOKEN_URL = `${AUTH_BASE}/oauth2/token`;

export const SCOPE = "openid email profile offline_access";

export const DEFAULT_PORT = 53_682;

export const DEFAULT_LOGIN_TIMEOUT_SECONDS = 600;

export const redirectUriFor = (port: number): string =>
  `http://127.0.0.1:${port}/callback`;

export const authorizeUrl = (parameters: {
  readonly clientId: string;
  readonly codeChallenge: string;
  readonly redirectUri: string;
  readonly state: string;
}): string => {
  const url = new URL(AUTHORIZE_URL);

  url.search = new URLSearchParams({
    client_id: parameters.clientId,
    code_challenge: parameters.codeChallenge,
    code_challenge_method: "S256",
    redirect_uri: parameters.redirectUri,
    resource: MCP_URL,
    response_type: "code",
    scope: SCOPE,
    state: parameters.state,
  }).toString();

  return url.toString();
};

export type CallbackOutcome = Data.TaggedEnum<{
  Code: { readonly code: string };
  Denied: { readonly reason: string };
  Ignored: { readonly path: string };
  StateMismatch: { readonly received: string | null };
}>;

export const Callback = Data.taggedEnum<CallbackOutcome>();

export const interpretCallback = (
  requestUrl: string,
  expectedState: string
): CallbackOutcome => {
  const url = new URL(requestUrl, "http://127.0.0.1");

  if (url.pathname !== "/callback") {
    return Callback.Ignored({ path: url.pathname });
  }

  const error = Option.fromNullishOr(url.searchParams.get("error"));

  if (Option.isSome(error)) {
    return Callback.Denied({
      reason: url.searchParams.get("error_description") ?? error.value,
    });
  }

  const received = url.searchParams.get("state");

  if (received !== expectedState) {
    return Callback.StateMismatch({ received });
  }

  const code = url.searchParams.get("code");

  return code === null
    ? Callback.Ignored({ path: url.pathname })
    : Callback.Code({ code });
};
