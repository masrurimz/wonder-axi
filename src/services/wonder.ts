import {
  Context,
  Data,
  Duration,
  Effect,
  Layer,
  Match,
  Option,
  Ref,
  Schema,
} from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";

import { AuthError } from "../domain/errors/auth-error.ts";
import { NetworkError } from "../domain/errors/network-error.ts";
import { RefusedError } from "../domain/errors/refused-error.ts";
import type { StorageError } from "../domain/errors/storage-error.ts";
import {
  MCP_URL,
  parseRpcResponse,
  PROTOCOL_VERSION,
  splitContent,
  ToolResultSchema,
  ToolsPageSchema,
} from "../domain/mcp.ts";
import type { ToolDef, ToolResult } from "../domain/mcp.ts";
import type { JsonObject } from "../domain/tool-args.ts";
import { Auth } from "./auth.ts";
import { send } from "./http-reply.ts";
import type { HttpReply } from "./http-reply.ts";

export type WonderFailure =
  | AuthError
  | NetworkError
  | StorageError
  | RefusedError;

type RpcStep = Data.TaggedEnum<{
  Ok: { readonly result: Schema.Json };
  SessionGone: { readonly status: number };
  Unauthorized: { readonly status: number };
}>;

const Step = Data.taggedEnum<RpcStep>();

const REQUEST_TIMEOUT = Duration.seconds(120);

const MAX_RETRIES = 1;

const CLIENT_VERSION = "0.1.0";

const unexpected = (detail: string) =>
  new NetworkError({
    fix: "Retry; if it persists, run `wonder-axi guide` to confirm the server is reachable.",
    message: `Wonder's MCP server returned an unexpected response: ${detail}`,
  });

const httpFailure = (reply: HttpReply) =>
  new NetworkError({
    fix:
      reply.status === 429
        ? "Rate limited. Wait a minute and retry."
        : "Retry; if it persists, check your network.",
    message: `Wonder's MCP server answered ${reply.status}: ${reply.text.slice(0, 300)}`,
  });

const isSessionGone = (reply: HttpReply): boolean =>
  reply.status === 404 ||
  (reply.status === 400 && /session/iu.test(reply.text));

const decodeResult = <S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  value: Schema.Json
) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError(() => unexpected("result did not match the MCP schema."))
  );

export class Wonder extends Context.Service<
  Wonder,
  {
    readonly callTool: (
      name: string,
      args: JsonObject
    ) => Effect.Effect<ToolResult, WonderFailure>;
    readonly listTools: Effect.Effect<readonly ToolDef[], WonderFailure>;
  }
>()("wonder-axi/Wonder") {
  static readonly layer = Layer.effect(
    Wonder,
    Effect.gen(function* build() {
      const client = yield* HttpClient.HttpClient;
      const auth = yield* Auth;
      const sessionRef = yield* Ref.make(Option.none<string>());
      const idRef = yield* Ref.make(0);

      const post = (
        token: string,
        sessionId: Option.Option<string>,
        body: JsonObject
      ) =>
        send(
          client,
          HttpClientRequest.post(MCP_URL).pipe(
            HttpClientRequest.bearerToken(token),
            HttpClientRequest.setHeader(
              "accept",
              "application/json, text/event-stream"
            ),
            HttpClientRequest.setHeader(
              "mcp-protocol-version",
              PROTOCOL_VERSION
            ),
            Option.isSome(sessionId)
              ? HttpClientRequest.setHeader("mcp-session-id", sessionId.value)
              : HttpClientRequest.setHeaders({}),
            HttpClientRequest.bodyJsonUnsafe(body)
          ),
          MCP_URL
        ).pipe(
          Effect.timeoutOrElse({
            duration: REQUEST_TIMEOUT,
            orElse: () =>
              Effect.fail(
                new NetworkError({
                  fix: "Retry the command.",
                  message: `Wonder did not answer within ${Duration.toSeconds(REQUEST_TIMEOUT)}s.`,
                })
              ),
          })
        );

      const nextId = Ref.updateAndGet(idRef, (id) => id + 1);

      const startSession = (token: string) =>
        Effect.gen(function* start() {
          const id = yield* nextId;

          const reply = yield* post(token, Option.none(), {
            id,
            jsonrpc: "2.0",
            method: "initialize",
            params: {
              capabilities: {},
              clientInfo: { name: "wonder-axi", version: CLIENT_VERSION },
              protocolVersion: PROTOCOL_VERSION,
            },
          });

          if (reply.status === 401) {
            return Option.none<string>();
          }

          if (reply.status < 200 || reply.status >= 300) {
            return yield* httpFailure(reply);
          }

          if (reply.sessionId === undefined) {
            return yield* unexpected("initialize returned no mcp-session-id.");
          }

          yield* post(token, Option.some(reply.sessionId), {
            jsonrpc: "2.0",
            method: "notifications/initialized",
          });
          yield* Ref.set(sessionRef, Option.some(reply.sessionId));

          return Option.some(reply.sessionId);
        });

      const attempt = (
        token: string,
        method: string,
        params: JsonObject
      ): Effect.Effect<RpcStep, NetworkError> =>
        Effect.gen(function* run() {
          const known = yield* Ref.get(sessionRef);

          const sessionId = Option.isSome(known)
            ? known
            : yield* startSession(token);

          if (Option.isNone(sessionId)) {
            return Step.Unauthorized({ status: 401 });
          }

          const id = yield* nextId;

          const reply = yield* post(token, sessionId, {
            id,
            jsonrpc: "2.0",
            method,
            params,
          });

          if (reply.status === 401) {
            return Step.Unauthorized({ status: reply.status });
          }

          if (isSessionGone(reply)) {
            return Step.SessionGone({ status: reply.status });
          }

          if (reply.status < 200 || reply.status >= 300) {
            return yield* httpFailure(reply);
          }

          const message = parseRpcResponse(reply.text, id);

          if (Option.isNone(message)) {
            return yield* unexpected(`no reply to ${method}.`);
          }

          if (message.value.error !== undefined) {
            return yield* new NetworkError({
              fix: "Run `wonder-axi guide <tool>` to check the arguments.",
              message: `Wonder rejected ${method}: ${message.value.error.message}`,
            });
          }

          return Step.Ok({ result: message.value.result ?? null });
        });

      const rpc = (
        method: string,
        params: JsonObject
      ): Effect.Effect<Schema.Json, WonderFailure> => {
        const go = (
          getToken: Effect.Effect<
            string,
            AuthError | NetworkError | StorageError
          >,
          retries: number
        ): Effect.Effect<Schema.Json, WonderFailure> =>
          Effect.gen(function* step() {
            const token = yield* getToken;
            const outcome = yield* attempt(token, method, params);

            return yield* Match.value(outcome).pipe(
              Match.tagsExhaustive({
                Ok: ({ result }) => Effect.succeed(result),
                SessionGone: () =>
                  retries > 0
                    ? Ref.set(sessionRef, Option.none()).pipe(
                        Effect.andThen(go(auth.accessToken, retries - 1))
                      )
                    : Effect.fail(
                        unexpected("the MCP session expired twice in a row.")
                      ),
                Unauthorized: () =>
                  retries > 0
                    ? go(auth.refreshAccessToken, retries - 1)
                    : Effect.fail(
                        new AuthError({
                          fix: "Run `wonder-axi auth login` to sign in again.",
                          message: "Wonder rejected the access token.",
                        })
                      ),
              })
            );
          });

        return go(auth.accessToken, MAX_RETRIES);
      };

      const callTool = Effect.fn("Wonder.callTool")(function* invoke(
        name: string,
        args: JsonObject
      ) {
        const result = yield* decodeResult(
          ToolResultSchema,
          yield* rpc("tools/call", { arguments: args, name })
        );

        if (result.isError === true) {
          return yield* new RefusedError({
            fix: "Fix the call as the message says, then retry. `wonder-axi guide <tool>` shows the arguments.",
            message: splitContent(result).texts.join("\n") || `${name} failed.`,
          });
        }

        return result;
      });

      const listPage = (
        cursor: Option.Option<string>
      ): Effect.Effect<readonly ToolDef[], WonderFailure> =>
        Effect.gen(function* page() {
          const listed = yield* decodeResult(
            ToolsPageSchema,
            yield* rpc(
              "tools/list",
              Option.isSome(cursor) ? { cursor: cursor.value } : {}
            )
          );

          if (listed.nextCursor === undefined) {
            return listed.tools;
          }

          const rest = yield* listPage(Option.some(listed.nextCursor));

          return [...listed.tools, ...rest];
        });

      return Wonder.of({ callTool, listTools: listPage(Option.none()) });
    })
  );
}
