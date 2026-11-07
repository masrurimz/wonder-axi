import { Console, Effect, Match, Option, Schema } from "effect";
import { Argument, Command, Flag } from "effect/cli";

import {
  decodeBasicInfo,
  describeHere,
  summarizeContext,
} from "../domain/context.ts";
import type { BasicInfo } from "../domain/context.ts";
import { UsageError } from "../domain/errors/usage-error.ts";
import { describeTool, summarizeTools } from "../domain/guide.ts";
import { oneLine, toon } from "../domain/render.ts";
import { Store } from "../services/store.ts";
import { Wonder } from "../services/wonder.ts";
import {
  callBasicInfo,
  decodeText,
  emit,
  FilesSchema,
  fullFlag,
  jsonFlag,
  pageFlag,
  printText,
  resolvePage,
  textOf,
  withOptional,
} from "./shared.ts";

const OpenSchema = Schema.Struct({
  branchName: Schema.optionalKey(Schema.String),
  fileId: Schema.String,
  fileName: Schema.String,
  organizationName: Schema.optionalKey(Schema.String),
  pageId: Schema.String,
});

const ArtboardsSchema = Schema.Array(
  Schema.Struct({
    dataNodeId: Schema.String,
    dataNodeLabel: Schema.optionalKey(Schema.String),
    descendantsCount: Schema.optionalKey(Schema.Finite),
    height: Schema.optionalKey(Schema.Finite),
    width: Schema.optionalKey(Schema.Finite),
  })
);

const TokensSchema = Schema.Struct({
  tokens: Schema.Array(
    Schema.Struct({
      aliasOf: Schema.optionalKey(Schema.String),
      cssVar: Schema.optionalKey(Schema.String),
      name: Schema.String,
      type: Schema.String,
      value: Schema.optionalKey(Schema.Json),
    })
  ),
});

const IconsSchema = Schema.Struct({
  icons: Schema.Array(
    Schema.Struct({
      componentName: Schema.optionalKey(Schema.String),
      name: Schema.String,
    })
  ),
});

const TreesSchema = Schema.Struct({
  results: Schema.Array(
    Schema.Struct({
      code: Schema.optionalKey(Schema.String),
      dataNodeId: Schema.String,
      tree: Schema.optionalKey(Schema.String),
    })
  ),
  warnings: Schema.optionalKey(Schema.Array(Schema.String)),
});

const displayValue = (value: Schema.Json | undefined): string =>
  Match.value(value).pipe(
    Match.when(Match.undefined, () => ""),
    Match.when(Match.string, (text) => text),
    Match.orElse((other) => JSON.stringify(other))
  );

const idsArgument = Argument.String("id").pipe(
  Argument.atLeast(1),
  Argument.withDescription("One or more data-node-id values.")
);

export const files = Command.make(
  "files",
  {
    json: jsonFlag,
    limit: Flag.Int("limit").pipe(
      Flag.withDefault(30),
      Flag.withDescription("Maximum rows to print.")
    ),
    name: Flag.String("name").pipe(
      Flag.optional,
      Flag.withDescription("Only files whose name contains this text.")
    ),
  },
  Effect.fn(function* runFiles({ json, limit, name }) {
    const wonder = yield* Wonder;

    const result = yield* wonder.callTool(
      "list_files",
      withOptional({}, "nameContains", name)
    );

    const decoded = decodeText(FilesSchema, result);

    if (Option.isNone(decoded)) {
      yield* printText(textOf(result), false);

      return;
    }

    const all = decoded.value.files;

    yield* emit(
      {
        files: all.slice(0, limit).map((file) => ({
          id: file.fileId,
          name: file.fileName,
          org: file.organizationName ?? "",
        })),
        total: all.length,
      },
      all.length === 0
        ? [
            "No files matched. Run `wonder-axi files` without --name to list everything.",
          ]
        : ["Run `wonder-axi open <id>` to select a file and its landing page."],
      json
    );
  })
).pipe(Command.withDescription("List the Wonder files you can open."));

export const open = Command.make(
  "open",
  {
    fileId: Argument.String("fileId").pipe(
      Argument.withDescription("A fileId from `wonder-axi files`.")
    ),
    json: jsonFlag,
  },
  Effect.fn(function* runOpen({ fileId, json }) {
    const wonder = yield* Wonder;
    const store = yield* Store;
    const result = yield* wonder.callTool("open_file", { fileId });
    const opened = decodeText(OpenSchema, result);

    if (Option.isNone(opened)) {
      yield* printText(textOf(result), false);

      return;
    }

    const file = opened.value;

    yield* store.writeState({
      fileId: file.fileId,
      fileName: file.fileName,
      pageId: file.pageId,
    });

    const info = decodeBasicInfo(
      textOf(
        yield* wonder.callTool("get_basic_info", {
          fileId: file.fileId,
          pageId: file.pageId,
        })
      )
    );

    const pages = Option.match(info, {
      onNone: () => [],
      onSome: (basic) => basic.currentApplicationContext?.pages ?? [],
    });

    yield* emit(
      {
        file: `${file.fileName} (${file.fileId})`,
        org: file.organizationName ?? "",
        branch: file.branchName ?? "",
        currentPage: file.pageId,
        pages: pages.map((page) => ({ id: page.pageId, name: page.pageName })),
      },
      [
        "Run `wonder-axi context` to read the file, its tokens and its artboards.",
        "Run `wonder-axi use <pageId>` to switch pages.",
      ],
      json
    );
  })
).pipe(
  Command.withDescription("Select a file and set its landing page as current.")
);

export const use = Command.make(
  "use",
  {
    json: jsonFlag,
    pageId: Argument.String("pageId").pipe(
      Argument.withDescription("A pageId from `wonder-axi open` or `context`.")
    ),
  },
  Effect.fn(function* runUse({ json, pageId }) {
    const store = yield* Store;
    const previous = yield* store.readState;

    yield* store.writeState({ ...Option.getOrUndefined(previous), pageId });
    yield* emit(
      { page: pageId },
      ["Run `wonder-axi context` to read this page."],
      json
    );
  })
).pipe(Command.withDescription("Set the current page."));

export const here = Command.make(
  "here",
  { full: fullFlag, json: jsonFlag },
  Effect.fn(function* runHere({ full, json }) {
    const store = yield* Store;

    const info = yield* callBasicInfo.pipe(
      Effect.map((result) => decodeBasicInfo(textOf(result))),
      Effect.catchTag("RefusedError", () =>
        Effect.succeed(Option.none<BasicInfo>())
      )
    );

    const found = describeHere(Option.getOrUndefined(info), full);

    if (found.state !== undefined) {
      yield* store.writeState(found.state);
    }

    yield* emit(
      found.summary,
      [
        found.state === undefined
          ? "Run `wonder-axi files` then `wonder-axi open <id>` to pick a file."
          : "Run `wonder-axi context` to read this page, or `wonder-axi tree <id>` for a selected element.",
      ],
      json
    );
  })
).pipe(
  Command.withDescription(
    "Show the file, page and selection open in the Wonder app, and make that page current."
  )
);

export const context = Command.make(
  "context",
  {
    full: fullFlag,
    guidelines: Flag.Literals("guidelines", ["none", "fundamentals"]).pipe(
      Flag.withDefault("fundamentals"),
      Flag.withDescription(
        "Include the design fundamentals text (default) or skip it."
      )
    ),
    json: jsonFlag,
    page: pageFlag,
  },
  Effect.fn(function* runContext({ full, guidelines, json, page }) {
    const wonder = yield* Wonder;
    const pageId = yield* resolvePage(page);

    const text = textOf(
      yield* wonder.callTool("get_design_context", { guidelines, pageId })
    );

    if (json) {
      yield* Console.log(JSON.stringify({ text }, null, 2));

      return;
    }

    if (full) {
      yield* Console.log(text);

      return;
    }

    yield* Console.log(
      summarizeContext(
        text,
        12,
        "Pass --full for the design fundamentals text."
      )
    );
    yield* Console.log(
      toon({
        help: [
          "Run `wonder-axi skill <job>` before designing: new-design, edit-design, code-to-design, design-to-code, design-library, generate-assets",
          "Run `wonder-axi guide update_elements` before authoring JSX",
        ],
      })
    );
  })
).pipe(
  Command.withDescription(
    "Read the file, page, selection, artboards, tokens and icons in one call."
  )
);

export const skill = Command.make(
  "skill",
  {
    name: Argument.String("name").pipe(
      Argument.withDescription(
        "new-design, edit-design, code-to-design, design-to-code, design-library or generate-assets."
      )
    ),
  },
  Effect.fn(function* runSkill({ name }) {
    const wonder = yield* Wonder;

    yield* Console.log(
      textOf(yield* wonder.callTool("get_skills", { skill: name }))
    );
  })
).pipe(
  Command.withDescription(
    "Print Wonder's own instructions for a design job, in full."
  )
);

export const guide = Command.make(
  "guide",
  {
    tool: Argument.String("tool").pipe(
      Argument.optional,
      Argument.withDescription("A Wonder tool name. Omit to list every tool.")
    ),
  },
  Effect.fn(function* runGuide({ tool }) {
    const wonder = yield* Wonder;
    const tools = yield* wonder.listTools;

    if (Option.isNone(tool)) {
      yield* Console.log(summarizeTools(tools));
      yield* Console.log(
        toon({
          help: [
            "Run `wonder-axi guide <tool>` for a tool's full description and parameters",
          ],
        })
      );

      return;
    }

    const found = tools.find((candidate) => candidate.name === tool.value);

    yield* found === undefined
      ? Effect.fail(
          new UsageError({
            fix: `Run \`wonder-axi guide\` to list tools. Known: ${tools
              .map((candidate) => candidate.name)
              .join(", ")}.`,
            message: `Unknown tool "${tool.value}".`,
          })
        )
      : Console.log(describeTool(found));
  })
).pipe(
  Command.withDescription(
    "List Wonder's tools, or print one tool's description and parameters."
  )
);

export const artboards = Command.make(
  "artboards",
  { json: jsonFlag, page: pageFlag },
  Effect.fn(function* runArtboards({ json, page }) {
    const wonder = yield* Wonder;
    const pageId = yield* resolvePage(page);
    const result = yield* wonder.callTool("get_artboards", { pageId });
    const decoded = decodeText(ArtboardsSchema, result);

    if (Option.isNone(decoded)) {
      yield* printText(textOf(result), false);

      return;
    }

    yield* emit(
      {
        artboards: decoded.value.map((artboard) => ({
          id: artboard.dataNodeId,
          label: artboard.dataNodeLabel ?? "",
          nodes: artboard.descendantsCount ?? 0,
          size: `${artboard.width ?? "?"}x${artboard.height ?? "?"}`,
        })),
        total: decoded.value.length,
      },
      ["Run `wonder-axi tree <id>` to read an artboard's structure."],
      json
    );
  })
).pipe(Command.withDescription("List the page's artboards."));

const treeCommand = (
  name: "tree" | "code",
  tool: "get_element_tree" | "get_element_code",
  description: string
) =>
  Command.make(
    name,
    {
      depth: Flag.Int("depth").pipe(
        Flag.optional,
        Flag.withDescription("Levels below each id to include.")
      ),
      full: fullFlag,
      ids: idsArgument,
      page: pageFlag,
    },
    Effect.fn(function* runTree({ depth, full, ids, page }) {
      const wonder = yield* Wonder;
      const pageId = yield* resolvePage(page);

      const result = yield* wonder.callTool(
        tool,
        withOptional({ dataNodeIds: ids, pageId }, "depth", depth)
      );

      const decoded = decodeText(TreesSchema, result);

      if (Option.isNone(decoded)) {
        yield* printText(textOf(result), full);

        return;
      }

      for (const entry of decoded.value.results) {
        yield* printText(
          `# ${entry.dataNodeId}\n${entry.tree ?? entry.code ?? ""}`,
          full
        );
      }

      for (const warning of decoded.value.warnings ?? []) {
        yield* Console.error(`warning: ${oneLine(warning, 300)}`);
      }
    })
  ).pipe(Command.withDescription(description));

export const tree = treeCommand(
  "tree",
  "get_element_tree",
  "Print the element tree under one or more ids."
);

export const code = treeCommand(
  "code",
  "get_element_code",
  "Print the React+Tailwind JSX under one or more ids."
);

export const find = Command.make(
  "find",
  {
    full: fullFlag,
    limit: Flag.Int("limit").pipe(
      Flag.optional,
      Flag.withDescription("Maximum matches per query.")
    ),
    page: pageFlag,
    queries: Argument.String("query").pipe(
      Argument.atLeast(1),
      Argument.withDescription(
        "One or more independent searches. Quote multi-word queries."
      )
    ),
  },
  Effect.fn(function* runFind({ full, limit, page, queries }) {
    const wonder = yield* Wonder;
    const pageId = yield* resolvePage(page);

    const result = yield* wonder.callTool(
      "find_elements",
      withOptional({ pageId, queries }, "limit", limit)
    );

    yield* printText(textOf(result), full);
  })
).pipe(
  Command.withDescription("Find elements on the page by free-form queries.")
);

export const tokens = Command.make(
  "tokens",
  { json: jsonFlag, page: pageFlag },
  Effect.fn(function* runTokens({ json, page }) {
    const wonder = yield* Wonder;
    const pageId = yield* resolvePage(page);
    const result = yield* wonder.callTool("get_design_tokens", { pageId });
    const decoded = decodeText(TokensSchema, result);

    if (Option.isNone(decoded)) {
      yield* printText(textOf(result), false);

      return;
    }

    yield* emit(
      {
        tokens: decoded.value.tokens.map((token) => ({
          cssVar: token.cssVar ?? "",
          name: token.name,
          type: token.type,
          value:
            token.aliasOf === undefined
              ? displayValue(token.value)
              : `alias:${token.aliasOf}`,
        })),
        total: decoded.value.tokens.length,
      },
      [
        "Bind colors in JSX with a token's cssVar. Create missing tokens with `wonder-axi call create_design_tokens`.",
      ],
      json
    );
  })
).pipe(Command.withDescription("List the file's design tokens."));

export const icons = Command.make(
  "icons",
  { json: jsonFlag, page: pageFlag },
  Effect.fn(function* runIcons({ json, page }) {
    const wonder = yield* Wonder;
    const pageId = yield* resolvePage(page);
    const result = yield* wonder.callTool("get_icons", { pageId });
    const decoded = decodeText(IconsSchema, result);

    if (Option.isNone(decoded)) {
      yield* printText(textOf(result), false);

      return;
    }

    yield* emit(
      {
        icons: decoded.value.icons.map((icon) => ({
          component: icon.componentName ?? "",
          name: icon.name,
        })),
        total: decoded.value.icons.length,
      },
      [
        "Run `wonder-axi call get_icons --arg names='[\"<name>\"]'` for an icon's svg markup.",
      ],
      json
    );
  })
).pipe(Command.withDescription("List the file's icon library."));
