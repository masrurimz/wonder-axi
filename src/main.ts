import { NodeRuntime } from "@effect/platform-node";

import { program } from "./cli.ts";

NodeRuntime.runMain(program, { disableErrorReporting: true });
