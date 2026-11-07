import { Runtime, Schema } from "effect";

export class RefusedError extends Schema.TaggedError<RefusedError>()(
  "RefusedError",
  {
    fix: Schema.String,
    message: Schema.String,
  }
) {
  readonly kind = "refused";
  readonly [Runtime.errorExitCode] = 1;
}
