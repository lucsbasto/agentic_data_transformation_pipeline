# Project skills

Local skills loaded by Claude Code for this repo. Each subfolder contains a `SKILL.md` that enforces project-specific best practices.

| Skill | Scope | Triggered by |
|---|---|---|
| `python-best-practices` | All Python code | editing `.py`, pyproject config, tests |
| `polars-lazy-pipeline` | Data transforms | polars imports, parquet I/O, transform code |
| `llm-client-anthropic-compat` | LLM integration | LLMClient code, prompts, model routing |
| `agentic-loop` | Autonomous agent | scheduler, retry, diagnose, auto-fix |
| `medallion-data-layout` | Layer contracts | any work on Bronze/Silver/Gold |

## How they're used

Claude Code reads `SKILL.md` frontmatter (`name`, `description`) and surfaces the skill when description keywords match the current task. The body of `SKILL.md` is loaded into context when the skill is invoked.

## Editing

- Keep rules short + actionable. Target ~200 lines max per skill.
- Lead with binding rules. Examples come after.
- Never put secrets or user data in skill files (they get committed).

## Related docs

- Authoritative decisions: `.specs/project/STATE.md`
- Roadmap + milestones: `.specs/project/ROADMAP.md`
- Product requirements: `PRD.md`
