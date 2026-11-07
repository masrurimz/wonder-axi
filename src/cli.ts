import { NodeServices } from "@effect/platform-node";
import { Console, Effect, Layer, Schema } from "effect";
import { CliError, Command } from "effect/cli";
import { Digest, SecureRandom } from "effect/crypto";
import { FetchHttpClient } from "effect/http";

import { auth } from "./commands/auth.ts";
import { call } from "./commands/call.ts";
import { showHome } from "./commands/home.ts";
import {
  artboards,
  code,
  context,
  files,
  find,
  guide,
  icons,
  open,
  skill,
  tokens,
  tree,
  use,
} from "./commands/read.ts";
import { jsonFlag } from "./commands/shared.ts";
import {
  createArtboard,
  finish,
  screenshot,
  update,
} from "./commands/write.ts";
import type { AppError } from "./domain/errors/app-error.ts";
import { UsageError } from "./domain/errors/usage-error.ts";
import { toon } from "./domain/render.ts";
import { Auth } from "./services/auth.ts";
import { CallbackServer } from "./services/callback-server.ts";
import { Store } from "./services/store.ts";
import { Wonder } from "./services/wonder.ts";

export const VERSION = "0.1.0";

export const root = Command.make("wonder-axi", { json: jsonFlag }, ({ json }) =>
  showHome(json)
).pipe(
  Command.withDescription(
    "Drive Wonder (wonder.design) canvases from the shell. Run with no arguments for your account and files."
  ),
  Command.withSubcommands([
    files,
    open,
    use,
    context,
    skill,
    guide,
    artboards,
    tree,
    code,
    find,
    createArtboard,
    update,
    screenshot,
    finish,
    tokens,
    icons,
    call,
    auth,
  ])
);

const services = Wonder.layer.pipe(
  Layer.provideMerge(Auth.layer),
  Layer.provideMerge(
    Layer.mergeAll(
      FetchHttpClient.layer,
      SecureRandom.layerWebCrypto,
      Digest.layerWebCrypto,
      CallbackServer.layer,
      Store.layer
    )
  ),
  Layer.provideMerge(NodeServices.layer)
);

const reportApp = (error: AppError) =>
  Console.error(toon({ error: error.message, fix: error.fix })).pipe(
    Effect.andThen(Effect.fail(error))
  );

const isShowHelp = Schema.is(CliError.ShowHelp);

const cliMessage = (error: CliError.CliError): string =>
  isShowHelp(error)
    ? error.errors.map((cause) => cause.message).join(" ")
    : error.message;

const reportCli = (error: CliError.CliError) =>
  isShowHelp(error) && error.errors.length === 0
    ? Effect.void
    : Console.error(
        toon({
          error: cliMessage(error),
          fix: "Run `wonder-axi --help` or `<command> --help`.",
        })
      ).pipe(
        Effect.andThen(
          Effect.fail(
            new UsageError({
              fix: "Run with --help.",
              message: cliMessage(error),
            })
          )
        )
      );

export const program = Command.run(root, {
  renderErrors: false,
  version: VERSION,
}).pipe(
  Effect.catchTags({
    AuthError: reportApp,
    NetworkError: reportApp,
    RefusedError: reportApp,
    StorageError: reportApp,
    UsageError: reportApp,
  }),
  Effect.catchIf(CliError.isCliError, reportCli),
  Effect.provide(services)
);
