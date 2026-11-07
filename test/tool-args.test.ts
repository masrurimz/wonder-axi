import { Result } from "effect";
import { describe, expect, it } from "vitest";

import { parseRpcResponse } from "../src/domain/mcp.ts";
import { parseArgPairs } from "../src/domain/tool-args.ts";

const schema = {
  properties: {
    depth: { type: "number" },
    pageId: { type: "string" },
    queries: { type: "array" },
  },
};

describe("parseArgPairs", () => {
  it("keeps strings raw and parses other declared types as JSON", () => {
    expect(
      parseArgPairs(["pageId=abc", "depth=2", 'queries=["x","y"]'], schema)
    ).toStrictEqual(
      Result.succeed({ depth: 2, pageId: "abc", queries: ["x", "y"] })
    );
  });

  it("keeps values containing equals signs intact", () => {
    expect(parseArgPairs(["pageId=a=b"], schema)).toStrictEqual(
      Result.succeed({ pageId: "a=b" })
    );
  });

  it("names the known arguments when one is unknown", () => {
    const result = parseArgPairs(["nope=1"], schema);

    expect(Result.isFailure(result)).toBe(true);
    expect(Result.getOrElse(Result.flip(result), () => "")).toContain(
      "Known: depth, pageId, queries"
    );
  });

  it("rejects a non-JSON value for a numeric argument", () => {
    expect(Result.isFailure(parseArgPairs(["depth=deep"], schema))).toBe(true);
  });

  it("falls back to strings when the tool schema is unknown", () => {
    expect(parseArgPairs(["a=1"], {})).toStrictEqual(
      Result.succeed({ a: "1" })
    );
  });
});

describe("parseRpcResponse", () => {
  it("picks the matching message out of an SSE body", () => {
    const body =
      'event: message\ndata: {"jsonrpc":"2.0","id":2,"result":{"ok":true}}\n\n';

    const parsed = parseRpcResponse(body, 2);

    expect(parsed._tag).toBe("Some");
  });

  it("ignores messages for other request ids", () => {
    const body = 'data: {"jsonrpc":"2.0","id":9,"result":{}}\n\n';

    expect(parseRpcResponse(body, 2)._tag).toBe("None");
  });

  it("accepts a plain JSON body", () => {
    expect(
      parseRpcResponse('{"jsonrpc":"2.0","id":1,"result":{}}', 1)._tag
    ).toBe("Some");
  });
});
