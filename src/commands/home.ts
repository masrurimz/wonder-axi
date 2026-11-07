import { Effect, Option } from "effect";

import { Store } from "../services/store.ts";
import { Wonder } from "../services/wonder.ts";
import { decodeText, emit, FilesSchema, lookupAccount } from "./shared.ts";

const RECENT = 5;

export const showHome = (json: boolean) =>
  Effect.gen(function* overview() {
    const wonder = yield* Wonder;
    const store = yield* Store;

    const looked = yield* lookupAccount;

    const account = {
      email: looked.email,
      info: Option.getOrUndefined(looked.info),
    };

    const listed = decodeText(
      FilesSchema,
      yield* wonder.callTool("list_files", {})
    );

    const state = yield* store.readState;

    const files = Option.match(listed, {
      onNone: () => [],
      onSome: (decoded) => decoded.files,
    });

    yield* emit(
      {
        account: Option.getOrElse(account.email, () => "unknown"),
        organization:
          account.info?.currentApplicationContext?.currentOrganizationName ??
          files[0]?.organizationName ??
          "unknown",
        page: Option.match(state, {
          onNone: () => "none selected",
          onSome: (current) => `${current.fileName ?? "?"} (${current.pageId})`,
        }),
        files: files.slice(0, RECENT).map((file) => ({
          id: file.fileId,
          name: file.fileName,
          org: file.organizationName ?? "",
        })),
        totalFiles: files.length,
      },
      [
        files.length === 0
          ? "No files yet. Create one in Wonder, then run `wonder-axi files`."
          : "Run `wonder-axi open <id>` to select a file.",
        "Run `wonder-axi files --name <text>` to find a file by name.",
        "Run `wonder-axi guide` to list every Wonder tool.",
      ],
      json
    );
  });
