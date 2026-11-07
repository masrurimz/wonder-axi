import { Runtime, Schema } from "effect";

export class AuthError extends Schema.TaggedError<AuthError>()("AuthError", {
  fix: Schema.String,
  message: Schema.String,
}) {
  readonly [Runtime.errorExitCode] = 2;
}
