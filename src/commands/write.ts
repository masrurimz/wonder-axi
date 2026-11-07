import { Console, Effect } from "effect";
import { Argument, Command, Flag } from "effect/cli";

import { RefusedError } from "../domain/errors/refused-error.ts";
import { splitContent } from "../domain/mcp.ts";
import { format, unwrapTrees } from "../domain/render.ts";
import { Wonder } from "../services/wonder.ts";
import { saveImage } from "./images.ts";
import {
  fileFlag,
  fullFlag,
  jsonFlag,
  pageFlag,
  printText,
  readInput,
  resolvePage,
  textOf,
} from "./shared.ts";

export const createArtboard = Command.make(
  "create-artboard",
  { file: fileFlag, full: fullFlag, page: pageFlag },
  Effect.fn(function* runCreateArtboard({ file, full, page }) {
    const wonder = yield* Wonder;
    const pageId = yield* resolvePage(page);
    const code = yield* readInput(file);
    const result = yield* wonder.callTool("create_artboard", { code, pageId });

    yield* printText(unwrapTrees(textOf(result)), full);
    yield* Console.log(
      format(
        {},
        [
          "Run `wonder-axi screenshot <artboardId>` to check the render.",
          "Run `wonder-axi finish <artboardId>` when the design is done.",
        ],
        false
      )
    );
  })
).pipe(
  Command.withDescription(
    "Create an artboard from JSX on stdin or --file. Run `guide create_artboard` first."
  )
);

export const update = Command.make(
  "update",
  { file: fileFlag, full: fullFlag, page: pageFlag },
  Effect.fn(function* runUpdate({ file, full, page }) {
    const wonder = yield* Wonder;
    const pageId = yield* resolvePage(page);
    const edit = yield* readInput(file);
    const result = yield* wonder.callTool("update_elements", { edit, pageId });

    yield* printText(unwrapTrees(textOf(result)), full);
  })
).pipe(
  Command.withDescription(
    "Apply JSX directives from stdin or --file. Run `guide update_elements` first."
  )
);

export const screenshot = Command.make(
  "screenshot",
  {
    dataNodeId: Argument.String("dataNodeId").pipe(
      Argument.withDescription("The element to capture.")
    ),
    json: jsonFlag,
    mode: Flag.Literals("mode", ["thumbnail", "full"]).pipe(
      Flag.withDefault("thumbnail"),
      Flag.withDescription("Use full to judge fine detail.")
    ),
    out: Flag.String("out").pipe(
      Flag.optional,
      Flag.withDescription(
        "Where to write the image. Default ./.wonder/shots/."
      )
    ),
    page: pageFlag,
  },
  Effect.fn(function* runScreenshot({ dataNodeId, json, mode, out, page }) {
    const wonder = yield* Wonder;
    const pageId = yield* resolvePage(page);

    const result = yield* wonder.callTool("take_screenshot", {
      dataNodeId,
      mode,
      pageId,
    });

    const [image] = splitContent(result).images;

    if (image === undefined) {
      return yield* new RefusedError({
        fix: "Check the element id with `wonder-axi tree <id>`.",
        message: textOf(result) || "Wonder returned no image.",
      });
    }

    const saved = yield* saveImage(image, { out, stem: dataNodeId });

    yield* Console.log(
      format(
        { ...saved },
        [
          "Open the file to inspect the render, then run `wonder-axi finish <artboardId>`.",
        ],
        json
      )
    );

    return saved;
  })
).pipe(
  Command.withDescription(
    "Render an element to an image file. Never prints image data."
  )
);

export const finish = Command.make(
  "finish",
  {
    artboardId: Argument.String("artboardId").pipe(
      Argument.withDescription("The artboard root's data-node-id.")
    ),
    page: pageFlag,
  },
  Effect.fn(function* runFinish({ artboardId, page }) {
    const wonder = yield* Wonder;
    const pageId = yield* resolvePage(page);

    yield* Console.log(
      textOf(yield* wonder.callTool("finish_artboard", { artboardId, pageId }))
    );
  })
).pipe(
  Command.withDescription(
    "Mark an artboard finished and print the server's reply (a design link when the server has one)."
  )
);
