import type { Option } from "effect";
import { Array as Arr, Result, Schema } from "effect";

export const MCP_URL = "https://mcp.wonder.so/mcp";

export const PROTOCOL_VERSION = "2025-06-18";

const TextContentSchema = Schema.Struct({
  text: Schema.String,
  type: Schema.Literal("text"),
});

const ImageContentSchema = Schema.Struct({
  data: Schema.String,
  mimeType: Schema.String,
  type: Schema.Literal("image"),
});

const OtherContentSchema = Schema.Struct({ type: Schema.String });

export const ToolResultSchema = Schema.Struct({
  content: Schema.Array(
    Schema.Union([TextContentSchema, ImageContentSchema, OtherContentSchema])
  ),
  isError: Schema.optionalKey(Schema.Boolean),
  structuredContent: Schema.optionalKey(Schema.Json),
});

export type ToolResult = typeof ToolResultSchema.Type;

export const ToolDefSchema = Schema.Struct({
  annotations: Schema.optionalKey(
    Schema.Struct({ readOnlyHint: Schema.optionalKey(Schema.Boolean) })
  ),
  description: Schema.optionalKey(Schema.String),
  inputSchema: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  name: Schema.String,
});

export type ToolDef = typeof ToolDefSchema.Type;

export const ToolsPageSchema = Schema.Struct({
  nextCursor: Schema.optionalKey(Schema.String),
  tools: Schema.Array(ToolDefSchema),
});

const RpcMessageSchema = Schema.Struct({
  error: Schema.optionalKey(
    Schema.Struct({ code: Schema.Finite, message: Schema.String })
  ),
  id: Schema.optionalKey(Schema.Union([Schema.Finite, Schema.String])),
  result: Schema.optionalKey(Schema.Json),
});

export type RpcMessage = typeof RpcMessageSchema.Type;

const decodeRpcMessage = Schema.decodeUnknownResult(
  Schema.fromJsonString(RpcMessageSchema)
);

const sseDataPayloads = (body: string): readonly string[] =>
  body
    .replaceAll("\r\n", "\n")
    .split("\n\n")
    .map((event) =>
      event
        .split("\n")
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice(5).trimStart())
        .join("\n")
    )
    .filter((payload) => payload.length > 0);

export const parseRpcResponse = (
  body: string,
  requestId: number
): Option.Option<RpcMessage> => {
  const payloads = body.trimStart().startsWith("{")
    ? [body]
    : sseDataPayloads(body);

  return Arr.findFirst(
    Arr.filterMap(payloads, (payload) => decodeRpcMessage(payload)),
    (message) => message.id === requestId
  );
};

export interface ToolContent {
  readonly texts: readonly string[];
  readonly images: readonly {
    readonly data: string;
    readonly mimeType: string;
  }[];
}

export const splitContent = (result: ToolResult): ToolContent => ({
  images: Arr.filterMap(result.content, (item) =>
    "data" in item && item.type === "image"
      ? Result.succeed({ data: item.data, mimeType: item.mimeType })
      : Result.failVoid
  ),
  texts: Arr.filterMap(result.content, (item) =>
    "text" in item && item.type === "text"
      ? Result.succeed(item.text)
      : Result.failVoid
  ),
});
