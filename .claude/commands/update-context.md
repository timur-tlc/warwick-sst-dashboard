---
description: Update CLAUDE.md with current project status
---

Review the current state of the project and update CLAUDE.md to reflect reality.

**Steps:**

1. Read the current `CLAUDE.md` thoroughly
2. Check key data sources for current numbers:
   - Query BigQuery for current row counts: `SELECT COUNT(*) FROM sst_events.sessions`, `sst_events.events`, `sst_events.items`
   - Check the latest date in the events table
   - Check Athena SAL version from `athena_transformation_layer.sql`
3. Check recent git log for any significant changes since last update
4. Check `docs/` for any new or updated documentation
5. Update CLAUDE.md:
   - **Status line**: Update row counts, SAL version, date info
   - **Last Updated**: Set to today's date
   - **Next Steps**: Remove completed items, add new ones if evident
   - **Key Files**: Add any new important files, remove deleted ones
   - Keep the same structure and style
6. Show a brief summary of what changed
7. **Commit and push** — stage `CLAUDE.md` by name, commit with a one-line message describing what changed, and push to `origin`. Do this without asking for confirmation. Skip only if there are unrelated dirty working-tree changes that shouldn't be bundled in.

**Important:**
- Do NOT remove information unless it's clearly outdated or wrong
- Do NOT restructure the file — preserve the existing layout
- Keep it concise — CLAUDE.md should stay under 200 lines
- Use `source .venv/bin/activate` before running Python
- BigQuery project ID is `376132452327` (not the project name)
