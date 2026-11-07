import { Effect } from "effect";
import type {
  HttpClient,
  HttpClientError,
  HttpClientRequest,
} from "effect/http";

import { NetworkError } from "../domain/errors/network-error.ts";

export interface HttpReply {
  readonly status: number;
  readonly contentType: string;
  readonly sessionId: string | undefined;
  readonly text: string;
}

export const networkError =
  (url: string) => (cause: HttpClientError.HttpClientError) =>
    new NetworkError({
      fix: "Check your network connection and retry.",
      message: `Request to ${url} failed: ${cause.message}`,
    });

export const send = (
  client: HttpClient.HttpClient,
  request: HttpClientRequest.HttpClientRequest,
  url: string
): Effect.Effect<HttpReply, NetworkError> =>
  Effect.gen(function* exchange() {
    const response = yield* client.execute(request);
    const text = yield* response.text;

    return {
      contentType: response.headers["content-type"] ?? "",
      sessionId: response.headers["mcp-session-id"],
      status: response.status,
      text,
    };
  }).pipe(Effect.mapError(networkError(url)));
