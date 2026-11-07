import {
  Config,
  Context,
  Effect,
  FileSystem,
  Layer,
  Option,
  Path,
  Schema,
} from "effect";

import { CredentialsSchema } from "../domain/credentials.ts";
import type { Credentials } from "../domain/credentials.ts";
import { StorageError } from "../domain/errors/storage-error.ts";
import { SessionStateSchema } from "../domain/state.ts";
import type { SessionState } from "../domain/state.ts";

const FILE_MODE = 0o600;

const DIR_MODE = 0o700;

const storageFailure = (action: string, file: string) =>
  Effect.mapError(
    () =>
      new StorageError({
        fix: "Check that the path is readable and writable, or delete it and retry.",
        message: `Could not ${action} ${file}.`,
      })
  );

export class Store extends Context.Service<
  Store,
  {
    readonly credentialsPath: string;
    readonly readCredentials: Effect.Effect<
      Option.Option<Credentials>,
      StorageError
    >;
    readonly writeCredentials: (
      credentials: Credentials
    ) => Effect.Effect<void, StorageError>;
    readonly deleteCredentials: Effect.Effect<boolean, StorageError>;
    readonly readState: Effect.Effect<
      Option.Option<SessionState>,
      StorageError
    >;
    readonly writeState: (
      state: SessionState
    ) => Effect.Effect<void, StorageError>;
  }
>()("wonder-axi/Store") {
  static readonly layer = Layer.effect(
    Store,
    Effect.gen(function* build() {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* Config.String("HOME");

      const configHome = yield* Config.String("XDG_CONFIG_HOME").pipe(
        Config.withDefault(path.join(home, ".config"))
      );

      const stateHome = yield* Config.String("XDG_STATE_HOME").pipe(
        Config.withDefault(path.join(home, ".local", "state"))
      );

      const credentialsPath = path.join(
        configHome,
        "wonder-axi",
        "credentials.json"
      );

      const statePath = path.join(stateHome, "wonder-axi", "state.json");

      const readJson = <S extends Schema.ConstraintDecoder<unknown>>(
        file: string,
        schema: S
      ) =>
        Effect.gen(function* load() {
          if (!(yield* fs.exists(file))) {
            return Option.none<S["Type"]>();
          }

          const text = yield* fs.readFileString(file);

          const decoded = yield* Schema.decodeUnknownEffect(
            Schema.fromJsonString(schema)
          )(text);

          return Option.some(decoded);
        }).pipe(storageFailure("read", file));

      const writeJson = <S extends Schema.Constraint>(
        file: string,
        schema: S,
        value: S["Type"]
      ) =>
        Effect.gen(function* save() {
          yield* fs.makeDirectory(path.dirname(file), {
            mode: DIR_MODE,
            recursive: true,
          });

          const text = yield* Schema.encodeEffect(
            Schema.fromJsonString(schema)
          )(value);

          yield* fs.writeFileString(file, `${text}\n`, { mode: FILE_MODE });
          yield* fs.chmod(file, FILE_MODE);
        }).pipe(storageFailure("write", file));

      return Store.of({
        credentialsPath,
        deleteCredentials: Effect.gen(function* remove() {
          if (!(yield* fs.exists(credentialsPath))) {
            return false;
          }

          yield* fs.remove(credentialsPath);

          return true;
        }).pipe(storageFailure("delete", credentialsPath)),
        readCredentials: readJson(credentialsPath, CredentialsSchema),
        readState: readJson(statePath, SessionStateSchema),
        writeCredentials: (credentials) =>
          writeJson(credentialsPath, CredentialsSchema, credentials),
        writeState: (state) => writeJson(statePath, SessionStateSchema, state),
      });
    }).pipe(
      Effect.mapError(
        (cause) =>
          new StorageError({
            fix: "Set HOME or XDG_CONFIG_HOME and XDG_STATE_HOME.",
            message: `Could not resolve config directories: ${cause.message}`,
          })
      )
    )
  );
}
