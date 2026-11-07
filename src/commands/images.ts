import { Clock, Effect, FileSystem, Option, Path } from "effect";

import { RefusedError } from "../domain/errors/refused-error.ts";
import { UsageError } from "../domain/errors/usage-error.ts";
import { decodeImage } from "../domain/image.ts";

export interface SavedImage {
  readonly path: string;
  readonly format: string;
  readonly bytes: number;
  readonly size: string;
}

export const saveImage = (
  image: { readonly data: string; readonly mimeType: string },
  options: { readonly out: Option.Option<string>; readonly stem: string }
) =>
  Effect.gen(function* persist() {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const decoded = decodeImage(image.data, image.mimeType);

    if (Option.isNone(decoded)) {
      return yield* new RefusedError({
        fix: "Retry the screenshot.",
        message: "Wonder returned an image that is not valid base64.",
      });
    }

    const now = yield* Clock.currentTimeMillis;

    const target = Option.getOrElse(options.out, () =>
      path.join(
        ".wonder",
        "shots",
        `${options.stem}-${now}.${decoded.value.extension}`
      )
    );

    yield* fs.makeDirectory(path.dirname(target), { recursive: true });
    yield* fs.writeFile(target, decoded.value.bytes);

    const { height, width } = decoded.value;

    return {
      bytes: decoded.value.bytes.length,
      format: decoded.value.mimeType,
      path: target,
      size:
        Option.isSome(width) && Option.isSome(height)
          ? `${width.value}x${height.value}`
          : "unknown",
    } satisfies SavedImage;
  }).pipe(
    Effect.catchTag("PlatformError", (cause) =>
      Effect.fail(
        new UsageError({
          fix: "Pass --out with a writable path.",
          message: `Could not write the image: ${cause.message}`,
        })
      )
    )
  );
