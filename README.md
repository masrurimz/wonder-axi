# wonder-axi

A command-line tool that lets coding agents design on a [Wonder](https://wonder.design) canvas without loading Wonder's MCP server into their context.

Wonder's MCP server exposes 41 tools whose schemas total 73.7 KB (about 18k tokens) on every turn. `wonder-axi` wraps that hosted server. The agent runs shell commands and gets compact [TOON](https://github.com/toon-format/toon) output, and reads a tool's guidance only when it needs it. `wonder-axi guide` prints about 4.6 KB and `wonder-axi guide update_elements` about 2.3 KB.

It follows the [AXI](https://axi.md) principles: content-first home view, definitive empty states, structured errors with a `fix:` line, distinct exit codes, next-step hints.

## Install

Needs Node 20 or newer.

```sh
npx -y wonder-axi
```

`bunx wonder-axi` works too. For agent plugins, see [plugins/wonder-axi](plugins/wonder-axi/README.md).

## Sign in

```sh
wonder-axi auth login
```

Open the printed URL in a browser. The CLI listens on `127.0.0.1:53682` for the callback (`--port` changes it). To sign in from a browser on another machine, forward the port first: `ssh -R 53682:127.0.0.1:53682 <host>`.

Credentials live in `$XDG_CONFIG_HOME/wonder-axi/credentials.json` (mode 0600) and refresh automatically. `WONDER_TOKEN`, a Wonder personal access token, overrides the file and never refreshes. `wonder-axi auth status` shows the account and expiry. `wonder-axi auth logout` deletes the file.

## Use

```sh
wonder-axi                     # account, organization, recent files
wonder-axi files --name card   # find a file
wonder-axi open <fileId>       # select a file and its landing page
wonder-axi context             # file, page, artboards, tokens, icons
wonder-axi skill new-design    # Wonder's own instructions for a job
wonder-axi guide update_elements
wonder-axi create-artboard --file card.jsx
wonder-axi update --file edits.txt
wonder-axi screenshot <artboardId>
wonder-axi finish <artboardId>
```

`call <tool> [--arg key=value] [--args-json '{}']` reaches any server tool, so new tools work without a release. Commands that take a page use `--page`, else the page selected by `open` or `use`, kept in `$XDG_STATE_HOME/wonder-axi/state.json`. `--json` switches output to JSON. Errors print `error:` and `fix:` on stderr; exit codes are 1 usage, 2 auth, 3 refused by the server, 4 network.

`screenshot` writes the image under `./.wonder/shots/` and prints only its path, format and size. The default thumbnail is JPEG, `--mode full` is PNG.

## Develop

Bun for dev and tests, Node for the published bundle.

```sh
bun install
bun run check   # lint, fmt, typecheck, typecheck:fast, test
bun run build   # dist/wonder-axi.js
```

See [AGENTS.md](AGENTS.md) for conventions and [PROVENANCE.md](PROVENANCE.md) for borrowed code.

## License

MIT. Author: Muhammad Masruri ([masrurimz](https://github.com/masrurimz)).
