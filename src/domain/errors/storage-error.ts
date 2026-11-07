import { Runtime, Schema } from "effect";

export class StorageError extends Schema.TaggedError<StorageError>()(
  "StorageError",
  {
    fix: Schema.String,
    message: Schema.String,
  }
) {
  readonly [Runtime.errorExitCode] = 1;
}
