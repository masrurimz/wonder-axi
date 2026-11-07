# Provenance

wonder-axi bans comments in code (see AGENTS.md). Credit for borrowed code lives here instead.

| Code | Origin | Notes |
| --- | --- | --- |
| `oxlint.config.ts`, `oxfmt.config.ts`, the Effect diagnostics map in `tsconfig.json`, `scripts/vcs-command-policy.ts` | [rat-stack](https://github.com/joelhooks/rat-stack) `oxlint.config.ts`, `oxfmt.config.ts`, `tsconfig.base.json`, `scripts/vcs-command-policy.js` (MIT) | Via omp-agent-mesh, trimmed to the rules that apply to a single-package Bun repo. The policy is ported to TypeScript; prek replaces lefthook. |
| `scripts/oxlint-plugin-no-comments.ts`, `scripts/oxlint-plugin-patterns.ts` (`acquire-release-constructs-in-acquire-body`, `no-module-level-mutable-state` only) | rat-stack `scripts/` (MIT) | Copied via omp-agent-mesh; unused rules removed. |
| `scripts/oxlint-plugin-effect-tests.ts` | [t3code](https://github.com/pingdotgg/t3code) `oxlint-plugin-t3code/rules/no-manual-effect-runtime-in-tests.ts`, via rat-stack | Adapted. |
| `tools/oxlint/anti-slop/` | [dmmulroy/anti-slop](https://github.com/dmmulroy/anti-slop) at `c44ef22`, MIT, via rat-stack | Vendored and owned; see `UPSTREAM.md` there. Its files keep upstream's comments. |
| `.omp/extensions/vcs-policy.ts` | omp-agent-mesh `.omp/extensions/vcs-policy.ts` | Copied. |

The CLI design follows the [AXI](https://axi.md) principles and the output style of gh-axi. It talks to Wonder's public MCP server and OAuth endpoints; no Wonder code is vendored.
