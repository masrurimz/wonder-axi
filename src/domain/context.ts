import { Option, Result, Schema } from "effect";

import { capped, oneLine, toon } from "./render.ts";

const SECTION_TITLES = [
  "# Basic info",
  "# Artboards (active page)",
  "# Design guidelines",
] as const;

type SectionTitle = (typeof SECTION_TITLES)[number];

export const splitSections = (
  text: string
): Readonly<Partial<Record<SectionTitle, string>>> => {
  const lines = text.split("\n");

  const starts = lines.flatMap((line, index) => {
    const title = SECTION_TITLES.find(
      (candidate) => candidate === line.trimEnd()
    );

    return title === undefined ? [] : [[title, index] as const];
  });

  return Object.fromEntries(
    starts.map(([title, start], position) => [
      title,
      lines
        .slice(start + 1, starts[position + 1]?.[1] ?? lines.length)
        .join("\n")
        .trim(),
    ])
  );
};

const NamedSchema = Schema.Struct({ name: Schema.String });

const AppContextSchema = Schema.Struct({
  currentBranchName: Schema.optionalKey(Schema.String),
  currentFileId: Schema.optionalKey(Schema.String),
  currentFileName: Schema.optionalKey(Schema.String),
  currentOrganizationName: Schema.optionalKey(Schema.String),
  currentPageId: Schema.optionalKey(Schema.String),
  currentPageName: Schema.optionalKey(Schema.String),
  pages: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({ pageId: Schema.String, pageName: Schema.String })
    )
  ),
  selectedElements: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        dataNodeId: Schema.String,
        dataNodeLabel: Schema.optionalKey(Schema.String),
      })
    )
  ),
});

const BasicInfoSchema = Schema.Struct({
  currentApplicationContext: Schema.optionalKey(AppContextSchema),
  designTokens: Schema.optionalKey(Schema.Array(NamedSchema)),
  icons: Schema.optionalKey(Schema.Array(NamedSchema)),
  user: Schema.optionalKey(
    Schema.Struct({ email: Schema.optionalKey(Schema.String) })
  ),
});

export type BasicInfo = typeof BasicInfoSchema.Type;

const ArtboardsSchema = Schema.Array(
  Schema.Struct({
    dataNodeId: Schema.String,
    dataNodeLabel: Schema.optionalKey(Schema.String),
    height: Schema.optionalKey(Schema.Finite),
    width: Schema.optionalKey(Schema.Finite),
  })
);

export const decodeBasicInfo = (json: string): Option.Option<BasicInfo> =>
  Result.getSuccess(
    Schema.decodeUnknownResult(Schema.fromJsonString(BasicInfoSchema))(json)
  );

const ACCOUNT_PATTERN = /Connected Wonder account: "(?<email>[^"]+)"/u;

export const accountFromText = (text: string): Option.Option<string> =>
  Option.fromNullishOr(ACCOUNT_PATTERN.exec(text)?.groups?.email);

const TOKEN_PREVIEW = 6;

const labelled = (name: string | undefined, id: string | undefined) =>
  `${name ?? "?"} (${id ?? "?"})`;

const artboardRows = (artboardsText: string) =>
  Result.getSuccess(
    Schema.decodeUnknownResult(Schema.fromJsonString(ArtboardsSchema))(
      artboardsText
    )
  ).pipe(
    Option.map((list) =>
      list.map((artboard) => ({
        id: artboard.dataNodeId,
        label: artboard.dataNodeLabel ?? "",
        size: `${artboard.width ?? "?"}x${artboard.height ?? "?"}`,
      }))
    )
  );

const describeApp = (info: BasicInfo | undefined) => {
  const app = info?.currentApplicationContext;

  return {
    file:
      app === undefined
        ? null
        : labelled(app.currentFileName, app.currentFileId),
    branch: app?.currentBranchName ?? null,
    page:
      app === undefined
        ? null
        : labelled(app.currentPageName, app.currentPageId),
    pages: (app?.pages ?? []).map((page) => ({
      id: page.pageId,
      name: page.pageName,
    })),
    selection: (app?.selectedElements ?? []).map((element) => ({
      id: element.dataNodeId,
      label: element.dataNodeLabel ?? "",
    })),
  };
};

const describeLibrary = (info: BasicInfo | undefined) => {
  const tokens = info?.designTokens ?? [];

  return {
    tokens: tokens.length,
    tokenNames: tokens.slice(0, TOKEN_PREVIEW).map((token) => token.name),
    icons: (info?.icons ?? []).length,
  };
};

export const summarizeContext = (
  text: string,
  guidelinesLineCap: number,
  fullHint: string
): string => {
  const sections = splitSections(text);

  const info = Option.getOrUndefined(
    decodeBasicInfo(sections["# Basic info"] ?? "")
  );

  const artboardsText = sections["# Artboards (active page)"] ?? "";
  const artboards = artboardRows(artboardsText);
  const guidelines = sections["# Design guidelines"] ?? "";

  const body = toon({
    ...describeApp(info),
    artboards: Option.getOrElse(artboards, () => []),
    ...describeLibrary(info),
  });

  const unavailable =
    Option.isNone(artboards) && artboardsText.length > 0
      ? `\nartboardsNote: ${oneLine(artboardsText, 200)}`
      : "";

  const guidelinesBlock =
    guidelines.length === 0
      ? ""
      : `\n\n${capped(guidelines, guidelinesLineCap, fullHint).text}`;

  return `${body}${unavailable}${guidelinesBlock}`;
};
