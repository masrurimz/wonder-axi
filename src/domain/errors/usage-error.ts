import { Runtime, Schema } from "effect";

export class UsageError extends Schema.TaggedError<UsageError>()("UsageError", {
  fix: Schema.String,
  message: Schema.String,
}) {
  readonly kind = "usage";
  readonly [Runtime.errorExitCode] = 2;
}
