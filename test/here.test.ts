import { Option } from "effect";
import { describe, expect, it } from "vitest";

import { decodeBasicInfo, describeHere } from "../src/domain/context.ts";

const infoWith = (selectedElements: readonly object[]) =>
  Option.getOrUndefined(
    decodeBasicInfo(
      JSON.stringify({
        currentApplicationContext: {
          currentBranchName: "main",
          currentFileId: "f1",
          currentFileName: "Card",
          currentPageId: "p1",
          currentPageName: "Page 1",
          selectedElements,
        },
      })
    )
  );

describe("describeHere", () => {
  it("says so when Wonder has no file open", () => {
    expect(describeHere(undefined, false)).toStrictEqual({
      state: undefined,
      summary: { file: "none open in Wonder" },
    });
  });

  it("states an empty selection definitively", () => {
    const { summary, state } = describeHere(infoWith([]), false);

    expect(state).toStrictEqual({
      fileId: "f1",
      fileName: "Card",
      pageId: "p1",
    });
    expect(summary).toMatchObject({
      branch: "main",
      file: "Card",
      page: "Page 1",
      selection: "none",
    });
  });

  it("keeps every field the server returns for a selected element", () => {
    const { summary } = describeHere(
      infoWith([{ dataNodeId: "a1", dataNodeLabel: "Hero", type: "frame" }]),
      false
    );

    expect(summary).toMatchObject({
      selectedCount: 1,
      selection: [{ id: "a1", label: "Hero", type: "frame" }],
    });
  });

  it("caps a long selection and points at --full", () => {
    const many = Array.from({ length: 12 }, (_, index) => ({
      dataNodeId: `n${index}`,
    }));

    const capped = describeHere(infoWith(many), false).summary;
    const full = describeHere(infoWith(many), true).summary;

    expect(capped).toMatchObject({
      selectedCount: 12,
      selectionNote: "2 more. Pass --full for all.",
    });
    expect(full.selection).toHaveLength(12);
  });
});
