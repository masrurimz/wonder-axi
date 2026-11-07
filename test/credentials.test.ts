import { Option } from "effect";
import { describe, expect, it } from "vitest";

import {
  describeExpiry,
  emailFromIdToken,
  needsRefresh,
  REFRESH_WINDOW_MS,
} from "../src/domain/credentials.ts";

const NOW = 1_800_000_000_000;

describe("needsRefresh", () => {
  it("refreshes inside the five minute window", () => {
    expect(needsRefresh(NOW + REFRESH_WINDOW_MS - 1, NOW)).toBe(true);
  });

  it("does not refresh outside the window", () => {
    expect(needsRefresh(NOW + REFRESH_WINDOW_MS + 1000, NOW)).toBe(false);
  });

  it("refreshes tokens that already expired", () => {
    expect(needsRefresh(NOW - 1000, NOW)).toBe(true);
  });
});

describe("describeExpiry", () => {
  it("shows days and hours remaining", () => {
    expect(describeExpiry(NOW + (6 * 24 + 23) * 3_600_000, NOW).expiresIn).toBe(
      "6d 23h"
    );
  });

  it("marks past expiry as expired", () => {
    expect(describeExpiry(NOW - 90 * 60_000, NOW).expiresIn).toBe(
      "expired 1h 30m ago"
    );
  });
});

describe("emailFromIdToken", () => {
  it("reads the email claim from the JWT payload", () => {
    const payload = btoa(JSON.stringify({ email: "a@b.c" })).replaceAll(
      "=",
      ""
    );

    expect(emailFromIdToken(`h.${payload}.s`)).toStrictEqual(
      Option.some("a@b.c")
    );
  });

  it("returns none when the payload is missing", () => {
    expect(Option.isNone(emailFromIdToken("onlyone"))).toBe(true);
  });
});
