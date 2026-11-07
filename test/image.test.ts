import { Option } from "effect";
import { describe, expect, it } from "vitest";

import { decodeImage } from "../src/domain/image.ts";

const PNG_HEADER = [
  0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49,
  0x48, 0x44, 0x52, 0x00, 0x00, 0x01, 0x86, 0x00, 0x00, 0x00, 0xc8,
];

const toBase64 = (bytes: readonly number[]) =>
  btoa(String.fromCodePoint(...bytes));

describe("decodeImage", () => {
  it("reads PNG dimensions from the IHDR chunk", () => {
    const decoded = decodeImage(toBase64(PNG_HEADER), "image/png");

    expect(Option.isSome(decoded)).toBe(true);
    expect(Option.getOrUndefined(decoded)?.extension).toBe("png");
    expect(Option.getOrUndefined(decoded)?.width).toStrictEqual(
      Option.some(390)
    );
    expect(Option.getOrUndefined(decoded)?.height).toStrictEqual(
      Option.some(200)
    );
  });

  it("reads JPEG dimensions from the start-of-frame marker", () => {
    const jpeg = [
      0xff, 0xd8, 0xff, 0xe0, 0x00, 0x04, 0x00, 0x00, 0xff, 0xc0, 0x00, 0x11,
      0x08, 0x00, 0xc8, 0x01, 0x86, 0x03,
    ];

    const decoded = Option.getOrUndefined(
      decodeImage(toBase64(jpeg), "image/jpeg")
    );

    expect(decoded?.extension).toBe("jpg");
    expect(decoded?.width).toStrictEqual(Option.some(390));
    expect(decoded?.height).toStrictEqual(Option.some(200));
  });

  it("rejects data that is not valid base64", () => {
    expect(Option.isNone(decodeImage("not base64!!", "image/png"))).toBe(true);
  });

  it("falls back to .bin and unknown size for unrecognised content", () => {
    const decoded = Option.getOrUndefined(
      decodeImage(toBase64([1, 2, 3]), "application/octet-stream")
    );

    expect(decoded?.extension).toBe("bin");
    expect(Option.isNone(decoded?.width ?? Option.some(0))).toBe(true);
  });
});
