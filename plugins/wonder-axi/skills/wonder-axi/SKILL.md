---
name: wonder-axi
description: Wonder canvas design via the wonder-axi CLI. Use to design on a Wonder (wonder.design) canvas, turn code into design, turn a Wonder design into code, or act on what the user has selected in Wonder.
---

# wonder-axi

`wonder-axi` drives Wonder's hosted MCP server from the shell. Output is compact TOON; `--json` switches to JSON. It is not on npm. Install once with `npm i -g https://github.com/masrurimz/wonder-axi/archive/refs/heads/master.tar.gz` (Node 20+), then run `wonder-axi <command>`.

## Start here

Begin every Wonder task with `wonder-axi here`. It prints the file, page, branch and the user's current selection (`selection: none` when empty), and makes that page the default for later commands.

When the user says "this", "that", "selected" or "here", they mean that selection. Act on those ids (`tree <id>`, `code <id>`, `update`). With `selection: none`, ask what they mean.

`here` reports `file: none open in Wonder` when nothing is open: run `files`, then `open <fileId>`.

## Sign in once

`wonder-axi auth status` shows the account. If it reports no credentials, run `wonder-axi auth login`, open the printed URL in any browser (a remote host needs `ssh -R 53682:127.0.0.1:53682 <host>`), and wait for the signed-in email. `WONDER_TOKEN` (a Wonder personal access token) overrides stored credentials.

## Rules

- Do what was asked and no more. A visual request is a design request: put it on the canvas. Write code only when asked. Code-only work never touches the canvas.
- Touch only the artboards, elements and files the task names or clearly implies. Report other problems you notice instead of fixing them.
- Commit, push or open a PR only when asked.
- Code to design: list the real UI first (controls, rows, text, icons, layout) and keep all of it. Bind colors, sizes and fonts to the file's tokens, creating missing tokens at the source values. Use the repo's icons. Never invent UI or values.
- Design to code: the design is the spec. Reuse or extend existing components through props or variants. Use the repo's tokens, never Wonder `var(--...)`, raw hex or px.
- Pick sensible defaults, and say which you picked.
- Always pass the pageId you are working on. Element ids are unique only within a page.
- Screenshot your result and look at it before reporting.

## Workflow

The server's text is the authority. `guide` and `skill` print it live.

1. `wonder-axi here`, then `wonder-axi context` for tokens, artboards and icons.
2. Load exactly one job skill before design work: `wonder-axi skill <job>` with `code-to-design`, `design-to-code`, `new-design`, `edit-design`, `design-library` or `generate-assets`. Read it in full. Quick text, color, spacing or icon tweaks need none.
3. `wonder-axi guide update_elements` and `guide create_artboard` before authoring JSX.
4. Create tokens first (`call create_design_tokens`), then bind colors with their `cssVar`.
5. Duplicate an artboard before substantive edits to existing work.
6. `create-artboard --file card.jsx`, then `update --file edits.txt`, one section per call.
7. `screenshot <artboardId>`, look at the image, fix. Three fix attempts at most, then report.
8. `finish <artboardId>` once per artboard, after its last edit.

## Commands

`here`, `files`, `open`, `use`, `context`, `skill`, `guide [tool]`, `artboards`, `tree`, `code`, `find`, `create-artboard`, `update`, `screenshot`, `finish`, `tokens`, `icons`, and `call <tool> [--arg key=value] [--args-json '{}']` for any server tool. `wonder-axi <command> --help` lists flags.

Commands that take a page use `--page`, else the page set by `here`, `open` or `use`.

## Output

- `screenshot` writes the image to `./.wonder/shots/` and prints its path and size. Open the file to inspect it.
- Long text is cut with a hint; `--full` prints all of it.
- Errors print `error:`, `kind:` and `fix:` on stdout. Exit 2 is a usage error, 1 any other failure. `kind: refused` names the server rule to fix.
