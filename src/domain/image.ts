import { Option, Result } from "effect";
import { Base64 } from "effect/encoding";

export interface DecodedImage {
  readonly bytes: Uint8Array;
  readonly mimeType: string;
  readonly extension: string;
  readonly width: Option.Option<number>;
  readonly height: Option.Option<number>;
}

const EXTENSIONS = new Map([
  ["image/gif", "gif"],
  ["image/jpeg", "jpg"],
  ["image/jpg", "jpg"],
  ["image/png", "png"],
  ["image/svg+xml", "svg"],
  ["image/webp", "webp"],
]);

const PNG_MAGIC = [0x89, 0x50, 0x4e, 0x47];

const PNG_HEADER_BYTES = 24;

const JPEG_MARKER_BYTES = 9;

const NON_FRAME_MARKERS = new Set([0xc4, 0xc8, 0xcc]);

const viewOf = (bytes: Uint8Array) =>
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

const pngSize = (bytes: Uint8Array) => {
  const isPng =
    bytes.length >= PNG_HEADER_BYTES &&
    PNG_MAGIC.every((value, index) => bytes[index] === value);

  if (!isPng) {
    return Option.none<readonly [number, number]>();
  }

  const view = viewOf(bytes);

  return Option.some([view.getUint32(16), view.getUint32(20)] as const);
};

const isStartOfFrame = (marker: number): boolean =>
  marker >= 0xc0 && marker <= 0xcf && !NON_FRAME_MARKERS.has(marker);

const jpegSize = (bytes: Uint8Array) => {
  const none = Option.none<readonly [number, number]>();

  if (bytes[0] !== 0xff || bytes[1] !== 0xd8) {
    return none;
  }

  const view = viewOf(bytes);

  const scan = (offset: number): typeof none => {
    if (offset + JPEG_MARKER_BYTES > bytes.length || bytes[offset] !== 0xff) {
      return none;
    }

    return isStartOfFrame(view.getUint8(offset + 1))
      ? Option.some([
          view.getUint16(offset + 7),
          view.getUint16(offset + 5),
        ] as const)
      : scan(offset + 2 + view.getUint16(offset + 2));
  };

  return scan(2);
};

export const imageSize = (bytes: Uint8Array) =>
  Option.orElse(pngSize(bytes), () => jpegSize(bytes));

export const decodeImage = (
  data: string,
  mimeType: string
): Option.Option<DecodedImage> =>
  Result.getSuccess(Base64.decode(data)).pipe(
    Option.map((bytes) => {
      const size = imageSize(bytes);

      return {
        bytes,
        extension: EXTENSIONS.get(mimeType) ?? "bin",
        height: size.pipe(Option.map(([, height]) => height)),
        mimeType,
        width: size.pipe(Option.map(([width]) => width)),
      };
    })
  );
