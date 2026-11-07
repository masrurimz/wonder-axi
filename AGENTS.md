# wonder-axi

An [AXI](https://axi.md) CLI that drives Wonder (wonder.design) canvases through Wonder's hosted MCP server at `https://mcp.wonder.so/mcp`. Agents run shell commands and read compact TOON output instead of carrying the server's 41 tool schemas (73.7 KB) in context. Public, MIT. Not on npm: install with `npm i -g https://github.com/masrurimz/wonder-axi/archive/refs/heads/master.tar.gz`, which works because `dist/wonder-axi.js` is committed. The `github:masrurimz/wonder-axi` form is broken for global installs on npm 11 (it links a deleted temp clone), so do not document it. Also ships as a plugin in this repo (`.claude-plugin/marketplace.json`, `plugins/wonder-axi/`).

## Effect

This repo uses Effect 4.1.2 (exact pins). Before writing any Effect code, read `node_modules/effect/AGENTS.md` completely, and grep `node_modules/effect/src` for any API name it does not show. Remembered Effect 3 names (`Context.Tag`, `Effect.catchAll`, `Effect.fork`) do not compile here.

Architecture rules:

- Failures are `Schema.TaggedError` classes, one per file in `src/domain/errors/`, each carrying a `fix` hint, a `kind` (usage, auth, refused, network, storage) and its exit code through `Runtime.errorExitCode`: 2 usage, 1 everything else. Errors print as TOON `error:`, `kind:` and `fix:` on stdout, never stderr. `--version` and `-v` print the bare version.
- Services are `Context.Service` classes with `static layer`: `Auth` (OAuth code + PKCE, refresh, userinfo), `Wonder` (JSON-RPC over `effect/http`, session, tool calls), `Store` (credentials and state files), `CallbackServer` (loopback login listener).
- Schema decodes every external value at the boundary: server JSON, credentials, state, OAuth responses.
- The only runner is `NodeRuntime.runMain` in `src/main.ts`; `Effect.run*` appears nowhere else. No module-level `let`. No `any`.
- No comments in code (enforced, see Fence). Knowledge goes in names, tests, or the commit message.
- The server is authoritative for tool grammar. Never copy its descriptions into the CLI or the skill. `guide` and `skill` fetch them live. Unknown tools stay reachable through `call`.
- Secrets: never print tokens. Refresh tokens rotate, so the live credentials file is the only valid copy; never restore an older one.

## Checks

Run one after another, never in parallel:

```sh
bun run lint            # oxlint
bun run fmt             # oxfmt --check
bun run typecheck       # effect-tsrs diagnostics --strict
bun run typecheck:fast  # bun check --no-script --threads 4
bun run test            # vitest run --maxWorkers=2
bun run check           # all of the above, in order
bun run build           # dist/wonder-axi.js, Node >= 20 ESM bundle; commit it with src changes
```

On shared machines, wrap heavy runs: `systemd-run --user --scope -p MemoryMax=8G <cmd>`.

Tests cover pure logic where a consumer-visible bug is plausible. No wiring tests, no mock echoes.

## Layout

- `src/main.ts` entry, `src/cli.ts` command tree and error handling.
- `src/commands/` one file per command family. `src/domain/` Schemas, errors, pure rules. `src/services/` the services.
- `plugins/wonder-axi/` the plugin (one skill, no MCP server). Keep `SKILL.md` short and pointed at `guide` and `skill`.
- `.dev/` holds a live snapshot of the server's tool catalog for reference. Git-ignored, never shipped.

## Fence

Ported from rat-stack (https://ratstack.sh/AGENTS.md) via omp-agent-mesh; credits in `PROVENANCE.md`. `oxlint.config.ts` extends `ultracite/oxlint/core` (typeAware) and adds `no-comments/no-comments`, the vendored `anti-slop/*` and `anti-slop-effect/*` rules (`tools/oxlint/anti-slop/`, see `UPSTREAM.md`), `rat-stack-patterns/{no-module-level-mutable-state,acquire-release-constructs-in-acquire-body}`, and `effect-tests/no-manual-effect-runtime-in-tests`. Effect language-service severities in `tsconfig.json` are enforced by `effect-tsrs --strict`.

`.omp/extensions/vcs-policy.ts` (policy in `scripts/vcs-command-policy.ts`) blocks agent `bash` commands that pass `--no-verify` to git.

Gardener rule: the baseline is zero violations and only shrinks. Fix code rather than disable a rule. A targeted `oxlint-disable` or `@effect-diagnostics` directive needs `-- reason` and a true boundary.
