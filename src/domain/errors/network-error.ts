import { Runtime, Schema } from "effect";

export class NetworkError extends Schema.TaggedError<NetworkError>()(
  "NetworkError",
  {
    fix: Schema.String,
    message: Schema.String,
  }
) {
  readonly [Runtime.errorExitCode] = 4;
}
