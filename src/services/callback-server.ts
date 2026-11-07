// @effect-diagnostics-next-line nodeBuiltinImport:off -- the Node platform HTTP server layer takes a node:http server factory
import { createServer } from "node:http";

import { NodeHttpServer } from "@effect/platform-node";
import { Context, Deferred, Duration, Effect, Layer, Match } from "effect";
import { HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http";

import { AuthError } from "../domain/errors/auth-error.ts";
import { UsageError } from "../domain/errors/usage-error.ts";
import { interpretCallback } from "../domain/oauth.ts";

const page = (message: string, status: number) =>
  HttpServerResponse.text(
    `<!doctype html><meta charset="utf-8"><title>wonder-axi</title><body style="font-family:system-ui;max-width:32rem;margin:4rem auto"><h1>wonder-axi</h1><p>${message}</p></body>`,
    { contentType: "text/html", status }
  );

export class CallbackServer extends Context.Service<
  CallbackServer,
  {
    readonly awaitCode: (options: {
      readonly port: number;
      readonly state: string;
      readonly timeoutSeconds: number;
      readonly onListening: Effect.Effect<void>;
    }) => Effect.Effect<string, AuthError | UsageError>;
  }
>()("wonder-axi/CallbackServer") {
  static readonly layer = Layer.succeed(
    CallbackServer,
    CallbackServer.of({
      awaitCode: ({ onListening, port, state, timeoutSeconds }) =>
        Effect.gen(function* listen() {
          const result = yield* Deferred.make<string, AuthError>();

          const handler = Effect.gen(function* respond() {
            const request = yield* HttpServerRequest.HttpServerRequest;

            return yield* Match.value(
              interpretCallback(request.url, state)
            ).pipe(
              Match.tagsExhaustive({
                Code: ({ code }) =>
                  Deferred.succeed(result, code).pipe(
                    Effect.as(
                      page(
                        "Signed in. You can close this tab and return to your terminal.",
                        200
                      )
                    )
                  ),
                Denied: ({ reason }) =>
                  Deferred.fail(
                    result,
                    new AuthError({
                      fix: "Run `wonder-axi auth login` again and approve the request.",
                      message: `Wonder denied the sign-in: ${reason}`,
                    })
                  ).pipe(Effect.as(page(`Sign-in denied: ${reason}`, 200))),
                Ignored: () =>
                  Effect.succeed(
                    HttpServerResponse.text("Not found", { status: 404 })
                  ),
                StateMismatch: () =>
                  Deferred.fail(
                    result,
                    new AuthError({
                      fix: "Run `wonder-axi auth login` again and open the newest URL only.",
                      message:
                        "The callback state did not match this login attempt.",
                    })
                  ).pipe(Effect.as(page("State mismatch.", 400))),
              })
            );
          });

          yield* HttpServer.serveEffect(handler);
          yield* onListening;

          return yield* Deferred.await(result).pipe(
            Effect.timeoutOrElse({
              duration: Duration.seconds(timeoutSeconds),
              orElse: () =>
                Effect.fail(
                  new AuthError({
                    fix: "Run `wonder-axi auth login` again and finish the browser step sooner.",
                    message: `No sign-in callback arrived within ${timeoutSeconds}s.`,
                  })
                ),
            })
          );
        }).pipe(
          Effect.scoped,
          Effect.provide(
            NodeHttpServer.layer(createServer, { host: "127.0.0.1", port })
          ),
          Effect.catchTag("ServeError", () =>
            Effect.fail(
              new UsageError({
                fix: "Pick a free port with `--port <n>`.",
                message: `Could not listen on 127.0.0.1:${port}.`,
              })
            )
          )
        ),
    })
  );
}
