---
name: wonder-axi
description: Wonder canvas design via the wonder-axi CLI. Use to design on a Wonder (wonder.design) canvas, turn code into design, or turn a Wonder design into code.
---

# wonder-axi

`wonder-axi` drives Wonder's hosted MCP server from the shell. Output is compact TOON; `--json` switches to JSON. Run it as `npx -y wonder-axi <command>` (or `bunx wonder-axi`).

## Sign in once

`wonder-axi auth status` shows the account. If it reports no credentials, run `wonder-axi auth login`, open the printed URL in any browser (a remote host needs `ssh -R 53682:127.0.0.1:53682 <host>`), and wait for the signed-in email. `WONDER_TOKEN` (a Wonder personal access token) overrides stored credentials.

## Workflow

Wonder's server instructions prescribe this order; the server's text is the authority, `guide` and `skill` print it live.

1. `wonder-axi open <fileId>` (or `use <pageId>`), then `wonder-axi context`. Context first, before any design call.
2. `wonder-axi skill <job>` before design work (`new-design` for a fresh design). Read it in full.
3. `wonder-axi guide update_elements` and `guide create_artboard` before authoring JSX.
4. Create tokens first (`call create_design_tokens`), then bind colors with their `cssVar`.
5. Duplicate an artboard before substantive edits to existing work.
6. `create-artboard --file card.jsx`, then `update --file edits.txt` one section per call.
7. `screenshot <artboardId>`, look at the image, fix. Three fix attempts at most, then report.
8. `finish <artboardId>` once per artboard, after its last edit.

## Commands

`files`, `open`, `use`, `context`, `skill`, `guide [tool]`, `artboards`, `tree`, `code`, `find`, `create-artboard`, `update`, `screenshot`, `finish`, `tokens`, `icons`, and `call <tool> [--arg key=value] [--args-json '{}']` for any server tool. `wonder-axi <command> --help` lists flags.

Commands that take a page use `--page`, else the page set by `open` or `use`.

## Output rules

- `screenshot` writes the image to disk (`./.wonder/shots/`) and prints its path and size. Open the file to inspect it. Thumbnails come back as JPEG, `--mode full` as PNG.
- Long text is cut with a hint; `--full` prints all of it.
- Errors print `error:`, `kind:` and `fix:` on stdout. Exit codes: 2 usage error, 1 any other failure. A server refusal (`kind: refused`) names the rule to fix.
