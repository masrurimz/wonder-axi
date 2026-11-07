import { Result, Schema } from "effect";

const JsonObjectSchema = Schema.Record(Schema.String, Schema.Unknown);

export type JsonObject = typeof JsonObjectSchema.Type;

const decodeJsonObject = Schema.decodeUnknownResult(
  Schema.fromJsonString(JsonObjectSchema)
);

const decodeJsonValue = Schema.decodeUnknownResult(
  Schema.fromJsonString(Schema.Unknown)
);

const DeclaredParamsSchema = Schema.Struct({
  properties: Schema.Record(
    Schema.String,
    Schema.Struct({ type: Schema.optionalKey(Schema.String) })
  ),
});

export const parseJsonObject = (
  text: string
): Result.Result<JsonObject, string> =>
  Result.mapError(
    decodeJsonObject(text),
    () => "Arguments must be a JSON object."
  );

const propertyTypes = (
  inputSchema: JsonObject | undefined
): ReadonlyMap<string, string> => {
  const declared = Result.getOrUndefined(
    Schema.decodeUnknownResult(DeclaredParamsSchema)(inputSchema ?? {})
  );

  return new Map(
    Object.entries(declared?.properties ?? {}).map(([key, value]) => [
      key,
      value.type ?? "string",
    ])
  );
};

export const declaresParam = (
  inputSchema: JsonObject | undefined,
  name: string
): boolean => propertyTypes(inputSchema).has(name);

const coerce = (
  key: string,
  raw: string,
  type: string
): Result.Result<unknown, string> => {
  if (type === "string") {
    return Result.succeed(raw);
  }

  return Result.mapError(
    decodeJsonValue(raw),
    () => `--arg ${key}: expected ${type} (JSON), got "${raw}".`
  );
};

const parsePair = (
  pair: string,
  types: ReadonlyMap<string, string>
): Result.Result<readonly [string, unknown], string> => {
  const index = pair.indexOf("=");

  if (index <= 0) {
    return Result.fail(`--arg expects key=value, got "${pair}".`);
  }

  const key = pair.slice(0, index);
  const type = types.get(key);

  if (types.size > 0 && type === undefined) {
    return Result.fail(
      `Unknown argument "${key}". Known: ${[...types.keys()].join(", ")}.`
    );
  }

  return Result.map(
    coerce(key, pair.slice(index + 1), type ?? "string"),
    (value) => [key, value] as const
  );
};

export const parseArgPairs = (
  pairs: readonly string[],
  inputSchema: JsonObject | undefined
): Result.Result<JsonObject, string> => {
  const types = propertyTypes(inputSchema);

  return Result.map(
    Result.all(pairs.map((pair) => parsePair(pair, types))),
    (entries) => Object.fromEntries(entries)
  );
};
