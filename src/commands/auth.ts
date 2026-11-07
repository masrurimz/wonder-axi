import { Clock, Console, Effect, Option } from "effect";
import { Command, Flag } from "effect/cli";

import { describeExpiry } from "../domain/credentials.ts";
import {
  DEFAULT_LOGIN_TIMEOUT_SECONDS,
  DEFAULT_PORT,
} from "../domain/oauth.ts";
import { format } from "../domain/render.ts";
import { Auth } from "../services/auth.ts";
import { jsonFlag, lookupAccount } from "./shared.ts";

const emailOrNone = lookupAccount.pipe(
  Effect.map((account) => account.email),
  Effect.orElseSucceed(() => Option.none<string>())
);

export const login = Command.make(
  "login",
  {
    json: jsonFlag,
    port: Flag.Int("port").pipe(
      Flag.withDefault(DEFAULT_PORT),
      Flag.withDescription("Loopback port for the OAuth callback.")
    ),
    timeout: Flag.Int("timeout").pipe(
      Flag.withDefault(DEFAULT_LOGIN_TIMEOUT_SECONDS),
      Flag.withDescription("Seconds to wait for the browser callback.")
    ),
  },
  Effect.fn(function* runLogin({ json, port, timeout }) {
    const auth = yield* Auth;

    const result = yield* auth.login({
      onUrl: (url) =>
        Console.log(
          [
            "Open this URL in a browser to sign in to Wonder:",
            url,
            `Waiting on http://127.0.0.1:${port}/callback for up to ${timeout}s.`,
            `From another machine: ssh -R ${port}:127.0.0.1:${port} <this-host>, then open the URL there.`,
          ].join("\n")
        ),
      port,
      timeoutSeconds: timeout,
    });

    const email = Option.isSome(result.email)
      ? result.email
      : yield* emailOrNone;

    yield* Console.log(
      format(
        { signedIn: Option.getOrElse(email, () => "unknown") },
        ["Run `wonder-axi` to see your files."],
        json
      )
    );
  })
).pipe(
  Command.withDescription(
    "Sign in to Wonder with OAuth (authorization code + PKCE)."
  )
);

export const status = Command.make(
  "status",
  { json: jsonFlag },
  Effect.fn(function* runStatus({ json }) {
    const auth = yield* Auth;
    const email = yield* emailOrNone;
    const current = yield* auth.status;
    const now = yield* Clock.currentTimeMillis;

    const expiry = Option.match(current.expiresAt, {
      onNone: () => ({ expires: "never (personal access token)" }),
      onSome: (at) => describeExpiry(at, now),
    });

    const location =
      current.source === "file" ? current.credentialsPath : "WONDER_TOKEN";

    yield* Console.log(
      format(
        {
          email: Option.getOrElse(
            email,
            () => "unknown (the token was not accepted)"
          ),
          source: current.source,
          ...expiry,
          credentials: location,
        },
        Option.isNone(email)
          ? ["Run `wonder-axi auth login` to sign in again."]
          : [],
        json
      )
    );
  })
).pipe(
  Command.withDescription(
    "Show the signed-in account, token source and expiry."
  )
);

export const logout = Command.make(
  "logout",
  { json: jsonFlag },
  Effect.fn(function* runLogout({ json }) {
    const auth = yield* Auth;
    const removed = yield* auth.logout;

    yield* Console.log(
      format(
        {
          loggedOut: removed,
          note: removed ? "credentials deleted" : "no credentials were stored",
        },
        [],
        json
      )
    );
  })
).pipe(Command.withDescription("Delete the stored credentials."));

export const auth = Command.make("auth").pipe(
  Command.withDescription("Sign in, inspect or sign out."),
  Command.withSubcommands([login, status, logout])
);
