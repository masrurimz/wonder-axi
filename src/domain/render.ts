import { encode } from "@toon-format/toon";
import { Option, Schema } from "effect";

export const DEFAULT_LINE_CAP = 120;

export const toon = (value: Schema.Json): string => encode(value);

const withHints = (
  value: Schema.JsonObject,
  help: readonly string[]
): Schema.JsonObject =>
  help.length === 0
    ? value
    : Object.fromEntries([...Object.entries(value), ["help", help]]);

export const withHelp = (
  value: Schema.JsonObject,
  help: readonly string[]
): string => toon(withHints(value, help));

export const format = (
  value: Schema.JsonObject,
  help: readonly string[],
  json: boolean
): string =>
  json
    ? JSON.stringify(withHints(value, help), null, 2)
    : withHelp(value, help);

export interface Truncated {
  readonly text: string;
  readonly omittedLines: number;
}

export const truncateLines = (text: string, cap: number): Truncated => {
  const lines = text.split("\n");

  return lines.length <= cap
    ? { omittedLines: 0, text }
    : {
        omittedLines: lines.length - cap,
        text: lines.slice(0, cap).join("\n"),
      };
};

export const capped = (
  text: string,
  cap: number,
  fullHint: string
): { readonly text: string; readonly truncated: boolean } => {
  const cut = truncateLines(text, cap);

  return cut.omittedLines === 0
    ? { text, truncated: false }
    : {
        text: `${cut.text}\n... ${cut.omittedLines} more lines (${text.length} chars total). ${fullHint}`,
        truncated: true,
      };
};

export const firstSentence = (description: string): string => {
  const flat = description.replaceAll(/\s+/gu, " ").trim();
  const end = flat.search(/[.!](?:\s|$)/u);

  return end === -1 ? flat : flat.slice(0, end + 1);
};

export const oneLine = (text: string, max: number): string => {
  const flat = text.replaceAll(/\s+/gu, " ").trim();

  return flat.length <= max ? flat : `${flat.slice(0, max - 1)}…`;
};

const TreesReplySchema = Schema.Struct({
  trees: Schema.Array(Schema.String),
});

const JsonReplySchema = Schema.fromJsonString(
  Schema.Record(Schema.String, Schema.Unknown)
);

export const unwrapTrees = (text: string): string => {
  const reply = Schema.decodeUnknownOption(JsonReplySchema)(text);

  const trees = reply.pipe(
    Option.flatMap(Schema.decodeUnknownOption(TreesReplySchema))
  );

  if (Option.isNone(trees) || Option.isNone(reply)) {
    return text;
  }

  const rest = Object.fromEntries(
    Object.entries(reply.value).filter(([key]) => key !== "trees")
  );

  const body = trees.value.trees.join("\n");

  return Object.keys(rest).length === 0
    ? body
    : `${body}\n${JSON.stringify(rest)}`;
};
