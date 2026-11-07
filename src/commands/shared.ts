import {
  Console,
  Effect,
  FileSystem,
  Option,
  Result,
  Schema,
  Stdio,
  Stream,
} from "effect";
import { Flag } from "effect/cli";

import { accountFromText, decodeBasicInfo } from "../domain/context.ts";
import type { BasicInfo } from "../domain/context.ts";
import type { StorageError } from "../domain/errors/storage-error.ts";
import { UsageError } from "../domain/errors/usage-error.ts";
import { splitContent } from "../domain/mcp.ts";
import type { ToolResult } from "../domain/mcp.ts";
import { capped, DEFAULT_LINE_CAP, format } from "../domain/render.ts";
import type { JsonObject } from "../domain/tool-args.ts";
import { Auth } from "../services/auth.ts";
import { Store } from "../services/store.ts";
import { Wonder } from "../services/wonder.ts";

export const pageFlag = Flag.String("page").pipe(
  Flag.optional,
  Flag.withDescription("Page id. Defaults to the page set by `use` or `open`.")
);

export const fullFlag = Flag.Boolean("full").pipe(
  Flag.withDefault(false),
  Flag.withDescription("Print the complete text without truncation.")
);

export const jsonFlag = Flag.Boolean("json").pipe(
  Flag.withDefault(false),
  Flag.withDescription("Print JSON instead of TOON.")
);

export const fileFlag = Flag.String("file").pipe(
  Flag.optional,
  Flag.withDescription("Read input from this file instead of stdin.")
);

export const FilesSchema = Schema.Struct({
  files: Schema.Array(
    Schema.Struct({
      fileId: Schema.String,
      fileName: Schema.String,
      organizationName: Schema.optionalKey(Schema.String),
    })
  ),
});

export const textOf = (result: ToolResult): string =>
  splitContent(result).texts.join("\n");

export const decodeText = <S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  result: ToolResult
): Option.Option<S["Type"]> =>
  Result.getSuccess(
    Schema.decodeUnknownResult(Schema.fromJsonString(schema))(textOf(result))
  );

export const withOptional = (
  base: JsonObject,
  key: string,
  value: Option.Option<string | number>
): JsonObject =>
  Option.match(value, {
    onNone: () => base,
    onSome: (present) => ({ ...base, [key]: present }),
  });

export const resolvePage = (
  page: Option.Option<string>
): Effect.Effect<string, UsageError | StorageError, Store> =>
  Effect.gen(function* lookup() {
    if (Option.isSome(page)) {
      return page.value;
    }

    const store = yield* Store;
    const state = yield* store.readState;

    if (Option.isNone(state)) {
      return yield* new UsageError({
        fix: "Run `wonder-axi open <fileId>` or `wonder-axi use <pageId>`, or pass --page <pageId>.",
        message: "No page selected.",
      });
    }

    return state.value.pageId;
  });

export const emit = (
  value: Schema.JsonObject,
  help: readonly string[],
  json: boolean
) => Console.log(format(value, help, json));

export const printText = (
  text: string,
  full: boolean,
  hint = "Pass --full for the complete text."
) => Console.log(full ? text : capped(text, DEFAULT_LINE_CAP, hint).text);

export const readInput = (file: Option.Option<string>) =>
  Effect.gen(function* ingest() {
    if (Option.isSome(file)) {
      const fs = yield* FileSystem.FileSystem;

      return yield* fs.readFileString(file.value);
    }

    const stdio = yield* Stdio.Stdio;

    if (yield* stdio.stdinIsTerminal) {
      return yield* new UsageError({
        fix: "Pipe the input (`wonder-axi <command> < file`) or pass --file <path>.",
        message: "No input: stdin is a terminal and --file was not given.",
      });
    }

    return yield* Stream.mkString(Stream.decodeText(stdio.stdin));
  }).pipe(
    Effect.catchTag("PlatformError", (cause) =>
      Effect.fail(
        new UsageError({
          fix: "Check the --file path or the piped input.",
          message: `Could not read input: ${cause.message}`,
        })
      )
    )
  );

export const callBasicInfo = Effect.gen(function* basicInfo() {
  const wonder = yield* Wonder;
  const store = yield* Store;
  const state = yield* store.readState;

  return yield* wonder.callTool(
    "get_basic_info",
    Option.match(state, {
      onNone: () => ({}),
      onSome: (current) => ({ pageId: current.pageId }),
    })
  );
});

export const lookupAccount = Effect.gen(function* lookup() {
  const auth = yield* Auth;

  const basic = yield* callBasicInfo.pipe(
    Effect.map((result) => {
      const text = textOf(result);

      return { info: decodeBasicInfo(text), text };
    }),
    Effect.catchTag("RefusedError", (refused) =>
      Effect.succeed({
        info: Option.none<BasicInfo>(),
        text: refused.message,
      })
    )
  );

  const direct = yield* auth.accountEmail.pipe(
    Effect.orElseSucceed(() => Option.none<string>())
  );

  const email = direct.pipe(
    Option.orElse(() =>
      Option.flatMapNullishOr(basic.info, (found) => found.user?.email)
    ),
    Option.orElse(() => accountFromText(basic.text))
  );

  return { email, info: basic.info };
});
