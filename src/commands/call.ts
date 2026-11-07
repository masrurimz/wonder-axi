import { Console, Effect, FileSystem, Option, Result } from "effect";
import { Argument, Command, Flag } from "effect/cli";

import { UsageError } from "../domain/errors/usage-error.ts";
import { splitContent } from "../domain/mcp.ts";
import { toon } from "../domain/render.ts";
import { parseArgPairs, parseJsonObject } from "../domain/tool-args.ts";
import type { JsonObject } from "../domain/tool-args.ts";
import { Wonder } from "../services/wonder.ts";
import { saveImage } from "./images.ts";
import { fullFlag, printText, readInput } from "./shared.ts";

const STDIN = "-";

const usage = (message: string) =>
  new UsageError({
    fix: "Run `wonder-axi guide <tool>` for the tool's parameters.",
    message,
  });

const readBase = (
  argsJson: Option.Option<string>,
  argsFile: Option.Option<string>
) =>
  Effect.gen(function* base() {
    if (Option.isSome(argsJson)) {
      return argsJson.value;
    }

    if (Option.isNone(argsFile)) {
      return "{}";
    }

    if (argsFile.value === STDIN) {
      return yield* readInput(Option.none());
    }

    const fs = yield* FileSystem.FileSystem;

    return yield* fs
      .readFileString(argsFile.value)
      .pipe(Effect.mapError(() => usage(`Could not read ${argsFile.value}.`)));
  });

const orUsage = <A>(parsed: Result.Result<A, string>) =>
  Result.match(parsed, {
    onFailure: (message) => Effect.fail(usage(message)),
    onSuccess: (value) => Effect.succeed(value),
  });

export const call = Command.make(
  "call",
  {
    arg: Flag.String("arg").pipe(
      Flag.atLeast(0),
      Flag.withDescription(
        "key=value, repeatable. Values are coerced from the tool's schema."
      )
    ),
    argsFile: Flag.String("args-file").pipe(
      Flag.optional,
      Flag.withDescription(
        "Read the JSON arguments from a file, or `-` for stdin."
      )
    ),
    argsJson: Flag.String("args-json").pipe(
      Flag.optional,
      Flag.withDescription("Arguments as one JSON object.")
    ),
    full: fullFlag,
    tool: Argument.String("tool").pipe(
      Argument.withDescription(
        "Any Wonder tool name. `wonder-axi guide` lists them."
      )
    ),
  },
  Effect.fn(function* runCall({ arg, argsFile, argsJson, full, tool }) {
    const wonder = yield* Wonder;

    const base: JsonObject = yield* orUsage(
      parseJsonObject(yield* readBase(argsJson, argsFile))
    );

    const tools = arg.length === 0 ? [] : yield* wonder.listTools;

    const inputSchema = tools.find(
      (candidate) => candidate.name === tool
    )?.inputSchema;

    const pairs: JsonObject = yield* orUsage(parseArgPairs(arg, inputSchema));
    const result = yield* wonder.callTool(tool, { ...base, ...pairs });
    const { images, texts } = splitContent(result);

    for (const text of texts) {
      yield* printText(text, full);
    }

    for (const image of images) {
      const saved = yield* saveImage(image, { out: Option.none(), stem: tool });

      yield* Console.log(toon({ image: { ...saved } }));
    }

    if (result.structuredContent !== undefined) {
      yield* Console.log(toon({ structuredContent: result.structuredContent }));
    }

    if (
      texts.length === 0 &&
      images.length === 0 &&
      result.structuredContent === undefined
    ) {
      yield* Console.log("ok: the tool returned no content");
    }
  })
).pipe(
  Command.withDescription(
    "Call any Wonder tool with key=value or JSON arguments. New server tools work with no release."
  )
);
