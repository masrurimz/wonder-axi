import type { AuthError } from "./auth-error.ts";
import type { NetworkError } from "./network-error.ts";
import type { RefusedError } from "./refused-error.ts";
import type { StorageError } from "./storage-error.ts";
import type { UsageError } from "./usage-error.ts";

export type AppError =
  | UsageError
  | StorageError
  | AuthError
  | RefusedError
  | NetworkError;
