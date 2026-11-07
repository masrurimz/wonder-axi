import { Clock, Config, Context, Effect, Layer, Option, Schema } from "effect";
import { Digest, SecureRandom } from "effect/crypto";
import { Base64Url } from "effect/encoding";
import { HttpClient, HttpClientRequest } from "effect/http";

import {
  credentialsFromToken,
  emailFromIdToken,
  needsRefresh,
  RegistrationResponseSchema,
  TokenResponseSchema,
  UserInfoSchema,
} from "../domain/credentials.ts";
import { AuthError } from "../domain/errors/auth-error.ts";
import type { NetworkError } from "../domain/errors/network-error.ts";
import type { StorageError } from "../domain/errors/storage-error.ts";
import type { UsageError } from "../domain/errors/usage-error.ts";
import { MCP_URL } from "../domain/mcp.ts";
import {
  authorizeUrl,
  REGISTER_URL,
  redirectUriFor,
  SCOPE,
  TOKEN_URL,
  USERINFO_URL,
} from "../domain/oauth.ts";
import { CallbackServer } from "./callback-server.ts";
import { send } from "./http-reply.ts";
import { Store } from "./store.ts";

export type TokenSource = "env" | "file";

export interface AuthStatus {
  readonly source: TokenSource;
  readonly expiresAt: Option.Option<number>;
  readonly credentialsPath: string;
}

export interface LoginResult {
  readonly email: Option.Option<string>;
}

type TokenFailure = AuthError | NetworkError | StorageError;

const PKCE_VERIFIER_BYTES = 32;

const STATE_BYTES = 16;

const notSignedIn = () =>
  new AuthError({
    fix: "Run `wonder-axi auth login`, or set WONDER_TOKEN to a Wonder personal access token.",
    message: "Not signed in to Wonder.",
  });

const decodeBody = <S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  text: string
) =>
  Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(text).pipe(
    Effect.mapError(
      () =>
        new AuthError({
          fix: "Retry; if it persists, run `wonder-axi auth login` again.",
          message: "Wonder's auth server returned an unexpected response.",
        })
    )
  );

export class Auth extends Context.Service<
  Auth,
  {
    readonly accessToken: Effect.Effect<string, TokenFailure>;
    readonly refreshAccessToken: Effect.Effect<string, TokenFailure>;
    readonly accountEmail: Effect.Effect<Option.Option<string>, TokenFailure>;
    readonly status: Effect.Effect<AuthStatus, TokenFailure>;
    readonly login: (options: {
      readonly port: number;
      readonly timeoutSeconds: number;
      readonly onUrl: (url: string) => Effect.Effect<void>;
    }) => Effect.Effect<LoginResult, TokenFailure | UsageError>;
    readonly logout: Effect.Effect<boolean, StorageError>;
  }
>()("wonder-axi/Auth") {
  static readonly layer = Layer.effect(
    Auth,
    Effect.gen(function* build() {
      const client = yield* HttpClient.HttpClient;
      const store = yield* Store;
      const callback = yield* CallbackServer;
      const random = yield* SecureRandom.SecureRandom;
      const digest = yield* Digest.Digest;

      const envToken = yield* Config.String("WONDER_TOKEN").pipe(
        Config.option,
        Effect.map(Option.filter((token: string) => token.length > 0)),
        Effect.orDie
      );

      const postForm = (
        url: string,
        fields: Readonly<Record<string, string>>
      ) =>
        Effect.gen(function* exchange() {
          const reply = yield* send(
            client,
            HttpClientRequest.post(url).pipe(
              HttpClientRequest.acceptJson,
              HttpClientRequest.bodyUrlParams(fields)
            ),
            url
          );

          if (reply.status < 200 || reply.status >= 300) {
            return yield* new AuthError({
              fix: "Run `wonder-axi auth login` to sign in again.",
              message: `Wonder's auth server answered ${reply.status}: ${reply.text.slice(0, 300)}`,
            });
          }

          return reply.text;
        });

      const refresh = Effect.gen(function* renew() {
        const credentials = yield* store.readCredentials;

        if (Option.isNone(credentials)) {
          return yield* notSignedIn();
        }

        const text = yield* postForm(TOKEN_URL, {
          client_id: credentials.value.clientId,
          grant_type: "refresh_token",
          refresh_token: credentials.value.refreshToken,
          resource: MCP_URL,
        });

        const token = yield* decodeBody(TokenResponseSchema, text);
        const now = yield* Clock.currentTimeMillis;
        const next = credentialsFromToken(credentials.value, token, now);

        if (Option.isNone(next)) {
          return yield* notSignedIn();
        }

        yield* store.writeCredentials(next.value);

        return next.value.accessToken;
      });

      const accessToken = Effect.gen(function* current() {
        if (Option.isSome(envToken)) {
          return envToken.value;
        }

        const credentials = yield* store.readCredentials;

        if (Option.isNone(credentials)) {
          return yield* notSignedIn();
        }

        const now = yield* Clock.currentTimeMillis;

        return needsRefresh(credentials.value.expiresAt, now)
          ? yield* refresh
          : credentials.value.accessToken;
      });

      const refreshAccessToken = Option.isSome(envToken)
        ? Effect.fail(
            new AuthError({
              fix: "Create a new personal access token in Wonder and update WONDER_TOKEN.",
              message: "WONDER_TOKEN was rejected by Wonder.",
            })
          )
        : refresh;

      const status = Effect.gen(function* inspect() {
        if (Option.isSome(envToken)) {
          return {
            credentialsPath: store.credentialsPath,
            expiresAt: Option.none<number>(),
            source: "env" as const,
          };
        }

        const credentials = yield* store.readCredentials;

        if (Option.isNone(credentials)) {
          return yield* notSignedIn();
        }

        return {
          credentialsPath: store.credentialsPath,
          expiresAt: Option.some(credentials.value.expiresAt),
          source: "file" as const,
        };
      });

      const registerClient = (redirectUri: string) =>
        Effect.gen(function* enroll() {
          const reply = yield* send(
            client,
            HttpClientRequest.post(REGISTER_URL).pipe(
              HttpClientRequest.acceptJson,
              HttpClientRequest.bodyJsonUnsafe({
                client_name: "wonder-axi",
                grant_types: ["authorization_code", "refresh_token"],
                redirect_uris: [redirectUri],
                response_types: ["code"],
                scope: SCOPE,
                token_endpoint_auth_method: "none",
              })
            ),
            REGISTER_URL
          );

          if (reply.status < 200 || reply.status >= 300) {
            return yield* new AuthError({
              fix: "Retry later, or set WONDER_TOKEN to a Wonder personal access token.",
              message: `Client registration failed with ${reply.status}: ${reply.text.slice(0, 300)}`,
            });
          }

          const registration = yield* decodeBody(
            RegistrationResponseSchema,
            reply.text
          );

          return registration.client_id;
        });

      const randomToken = (size: number) =>
        random.randomBytes(size).pipe(Effect.map(Base64Url.encode));

      const login = Effect.fn("Auth.login")(function* signIn(options: {
        readonly port: number;
        readonly timeoutSeconds: number;
        readonly onUrl: (url: string) => Effect.Effect<void>;
      }) {
        const redirectUri = redirectUriFor(options.port);
        const existing = yield* store.readCredentials;

        const reusable = existing.pipe(
          Option.filter((stored) => stored.redirectUri === redirectUri)
        );

        const clientId = Option.isSome(reusable)
          ? reusable.value.clientId
          : yield* registerClient(redirectUri);

        const verifier = yield* randomToken(PKCE_VERIFIER_BYTES);
        const state = yield* randomToken(STATE_BYTES);

        const challenge = Base64Url.encode(
          yield* digest.digest(
            Digest.sha256,
            new TextEncoder().encode(verifier)
          )
        );

        const code = yield* callback.awaitCode({
          onListening: options.onUrl(
            authorizeUrl({
              clientId,
              codeChallenge: challenge,
              redirectUri,
              state,
            })
          ),
          port: options.port,
          state,
          timeoutSeconds: options.timeoutSeconds,
        });

        const text = yield* postForm(TOKEN_URL, {
          client_id: clientId,
          code,
          code_verifier: verifier,
          grant_type: "authorization_code",
          redirect_uri: redirectUri,
          resource: MCP_URL,
        });

        const token = yield* decodeBody(TokenResponseSchema, text);
        const now = yield* Clock.currentTimeMillis;

        const credentials = credentialsFromToken(
          { clientId, redirectUri },
          token,
          now
        );

        if (Option.isNone(credentials)) {
          return yield* new AuthError({
            fix: "Run `wonder-axi auth login` again and approve offline access.",
            message: "Wonder did not return a refresh token.",
          });
        }

        yield* store.writeCredentials(credentials.value);

        return {
          email: Option.fromUndefinedOr(token.id_token).pipe(
            Option.flatMap(emailFromIdToken)
          ),
        };
      });

      const accountEmail = Effect.gen(function* who() {
        const token = yield* accessToken;

        const reply = yield* send(
          client,
          HttpClientRequest.get(USERINFO_URL).pipe(
            HttpClientRequest.acceptJson,
            HttpClientRequest.bearerToken(token)
          ),
          USERINFO_URL
        );

        if (reply.status < 200 || reply.status >= 300) {
          return Option.none<string>();
        }

        return Schema.decodeUnknownOption(
          Schema.fromJsonString(UserInfoSchema)
        )(reply.text).pipe(Option.flatMapNullishOr((info) => info.email));
      });

      return Auth.of({
        accessToken,
        accountEmail,
        login,
        logout: store.deleteCredentials,
        refreshAccessToken,
        status,
      });
    })
  );
}
