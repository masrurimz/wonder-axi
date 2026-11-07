import { Schema } from "effect";

export const SessionStateSchema = Schema.Struct({
  fileId: Schema.optionalKey(Schema.String),
  fileName: Schema.optionalKey(Schema.String),
  pageId: Schema.String,
});

export type SessionState = typeof SessionStateSchema.Type;
