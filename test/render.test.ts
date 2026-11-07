import { describe, expect, it } from "vitest";

import { capped, firstSentence, format } from "../src/domain/render.ts";

describe("capped", () => {
  it("leaves short text untouched", () => {
    expect(capped("a\nb", 5, "hint")).toStrictEqual({
      text: "a\nb",
      truncated: false,
    });
  });

  it("cuts at the cap and reports the omitted lines with the hint", () => {
    const result = capped("1\n2\n3\n4\n5", 2, "Pass --full.");

    expect(result.truncated).toBe(true);
    expect(result.text).toBe(
      "1\n2\n... 3 more lines (9 chars total). Pass --full."
    );
  });
});

describe("firstSentence", () => {
  it("returns the first sentence of a multi-line description", () => {
    expect(
      firstSentence("Creates an artboard\nfrom JSX. Then more text.")
    ).toBe("Creates an artboard from JSX.");
  });

  it("does not split on dots inside words", () => {
    expect(firstSentence("Uses file.tokens for lookup")).toBe(
      "Uses file.tokens for lookup"
    );
  });
});

describe("format", () => {
  it("adds next-step hints as a help list in JSON mode", () => {
    expect(JSON.parse(format({ a: 1 }, ["do x"], true))).toStrictEqual({
      a: 1,
      help: ["do x"],
    });
  });

  it("omits help when there are no hints", () => {
    expect(JSON.parse(format({ a: 1 }, [], true))).toStrictEqual({ a: 1 });
  });

  it("emits TOON by default", () => {
    expect(format({ a: 1, b: "x" }, [], false)).toBe("a: 1\nb: x");
  });
});
