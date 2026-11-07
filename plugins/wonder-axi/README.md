# wonder-axi plugin

Gives a coding agent the `wonder-axi` skill. The skill teaches the agent to design on a [Wonder](https://wonder.design) canvas through the `wonder-axi` CLI instead of loading Wonder's MCP server, whose 41 tool schemas weigh 73.7 KB on every turn.

The plugin ships a skill only. It has no `.mcp.json`, hooks, or commands. The CLI itself runs through `npx -y wonder-axi`, so Node 20 or newer is the only requirement.

## Install

Claude Code:

```
/plugin marketplace add masrurimz/wonder-axi
/plugin install wonder-axi@wonder-axi
```

omp:

```
/marketplace add masrurimz/wonder-axi
/marketplace install wonder-axi@wonder-axi
```

Then sign in once with `npx -y wonder-axi auth login`.

## Contents

- `skills/wonder-axi/SKILL.md`: when to use the CLI, the design workflow, and the command list. Grammar for Wonder's tools is not copied here. `wonder-axi guide <tool>` and `wonder-axi skill <job>` print the live text from Wonder's server.
