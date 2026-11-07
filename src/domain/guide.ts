import { Result, Schema } from "effect";

import type { ToolDef } from "./mcp.ts";
import { firstSentence, oneLine, toon } from "./render.ts";

const PropertySchema = Schema.Struct({
  description: Schema.optionalKey(Schema.String),
  enum: Schema.optionalKey(Schema.Array(Schema.Unknown)),
  items: Schema.optionalKey(
    Schema.Struct({ type: Schema.optionalKey(Schema.String) })
  ),
  type: Schema.optionalKey(
    Schema.Union([Schema.String, Schema.Array(Schema.String)])
  ),
});

const InputParamsSchema = Schema.Struct({
  properties: Schema.optionalKey(Schema.Record(Schema.String, PropertySchema)),
  required: Schema.optionalKey(Schema.Array(Schema.String)),
});

const typeLabel = (property: typeof PropertySchema.Type): string => {
  if (property.enum !== undefined) {
    return property.enum.map(String).join("|");
  }

  const base = [property.type ?? "any"].flat().join("|");

  return base === "array" && property.items?.type !== undefined
    ? `${property.items.type}[]`
    : base;
};

export const summarizeTools = (tools: readonly ToolDef[]): string =>
  toon({
    tools: tools.map((tool) => ({
      name: tool.name,
      readOnly: tool.annotations?.readOnlyHint === true,
      summary: oneLine(firstSentence(tool.description ?? ""), 110),
    })),
  });

export const describeTool = (tool: ToolDef): string => {
  const declared = Result.getOrUndefined(
    Schema.decodeUnknownResult(InputParamsSchema)(tool.inputSchema ?? {})
  );

  const required = new Set(declared?.required);

  const params = Object.entries(declared?.properties ?? {}).map(
    ([name, property]) => ({
      description: oneLine(firstSentence(property.description ?? ""), 120),
      name,
      required: required.has(name),
      type: typeLabel(property),
    })
  );

  const readOnly =
    tool.annotations?.readOnlyHint === true ? " (read-only)" : "";

  return [
    `# ${tool.name}${readOnly}`,
    "",
    tool.description ?? "",
    "",
    toon({ params }),
  ].join("\n");
};
