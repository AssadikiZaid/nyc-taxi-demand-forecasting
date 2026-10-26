"""Turn a sprint backlog file into GitHub issues on a GitHub Projects board.

Each task of ``docs/sprints/sprint-N-backlog.md`` (a ``### S1-01 · Title`` heading followed
by its bullets) becomes one issue with priority and area labels, in a milestone named after
the sprint, and is added to the project board. Running it again creates only what is
missing, so it is safe to re-run after adding tasks.

Needs the GitHub CLI, logged in with the ``project`` scope::

    gh auth login --web --scopes project
    python scripts/backlog_to_github.py docs/sprints/sprint-1-backlog.md
    python scripts/backlog_to_github.py docs/sprints/sprint-1-backlog.md --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_TITLE = "NYC taxi demand forecasting"
PRIORITY_COLORS = {"must": "b60205", "should": "fbca04", "could": "0e8a16"}
AREA_COLOR = "1d76db"

_TASK_HEADING = re.compile(r"^### (?P<key>S\d+-\d+) · (?P<title>.+)$")
_META_LINE = re.compile(
    r"^- (?P<priority>Must|Should|Could) · (?P<hours>[\d.]+) h · (?P<area>[\w-]+)$"
)
_SPRINT_TITLE = re.compile(r"^# (?P<sprint>Sprint \d+) backlog$")


@dataclass(frozen=True)
class Task:
    """One backlog task."""

    key: str
    title: str
    priority: str
    hours: float
    area: str
    body: str

    @property
    def issue_title(self) -> str:
        """Return the issue title, prefixed with the task key so it can be found again."""
        return f"{self.key} · {self.title}"

    @property
    def labels(self) -> list[str]:
        """Return the labels of the issue."""
        return [f"priority: {self.priority.lower()}", self.area]


def parse_backlog(text: str) -> tuple[str, list[Task]]:
    """Return the sprint name and the tasks of a backlog file.

    Raises:
        ValueError: If the sprint title or a task's priority line is missing.
    """
    lines = text.splitlines()
    sprint = next((m["sprint"] for line in lines if (m := _SPRINT_TITLE.match(line))), None)
    if sprint is None:
        raise ValueError("The backlog must start with a '# Sprint N backlog' title")

    tasks: list[Task] = []
    current: dict | None = None
    for line in [*lines, "## end"]:
        heading = _TASK_HEADING.match(line)
        # A task is its heading plus the bullet list under it (with indented continuations).
        outside_list = bool(line.strip()) and not line.startswith(("- ", "  "))
        if current is not None and (heading or outside_list):
            tasks.append(_build_task(current))
            current = None
        if heading:
            current = {"key": heading["key"], "title": heading["title"].strip(), "lines": []}
        elif current is not None:
            current["lines"].append(line)
    return sprint, tasks


def _build_task(raw: dict) -> Task:
    body_lines = list(raw["lines"])
    while body_lines and not body_lines[0].strip():
        body_lines.pop(0)
    if not body_lines or not (meta := _META_LINE.match(body_lines[0])):
        raise ValueError(f"{raw['key']}: the first line must read '- Must · 2 h · area'")
    body = "\n".join(body_lines[1:]).strip()
    footer = f"Priority: {meta['priority']} · Estimate: {meta['hours']} h · Area: {meta['area']}"
    return Task(
        key=raw["key"],
        title=raw["title"],
        priority=meta["priority"],
        hours=float(meta["hours"]),
        area=meta["area"],
        body=f"{body}\n\n{footer}".strip(),
    )


class GitHub:
    """Thin wrapper around the GitHub CLI; in dry-run mode it only prints the commands."""

    def __init__(self, dry_run: bool) -> None:
        """Set the mode."""
        self.dry_run = dry_run

    def run(self, *args: str, read_only: bool = False) -> str:
        """Run ``gh`` with ``args`` and return its output (nothing in dry-run mode)."""
        if self.dry_run and not read_only:
            print("gh", " ".join(args))
            return ""
        result = subprocess.run(["gh", *args], capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"gh {' '.join(args)} failed:\n{result.stderr.strip()}")
        return result.stdout


def sync(backlog: Path, gh: GitHub) -> None:
    """Create the milestone, labels, issues and project items that are missing."""
    sprint, tasks = parse_backlog(backlog.read_text(encoding="utf-8"))
    repo = json.loads(gh.run("repo", "view", "--json", "nameWithOwner,owner", read_only=True))
    owner, slug = repo["owner"]["login"], repo["nameWithOwner"]
    print(f"{slug}: {len(tasks)} tasks for {sprint}")

    milestones = json.loads(gh.run("api", f"repos/{slug}/milestones?state=all", read_only=True))
    if sprint not in {m["title"] for m in milestones}:
        gh.run("api", f"repos/{slug}/milestones", "-f", f"title={sprint}")

    for task in tasks:
        priority = task.labels[0].split(": ")[1]
        gh.run("label", "create", task.labels[0], "--color", PRIORITY_COLORS[priority], "--force")
    for area in sorted({task.area for task in tasks}):
        gh.run("label", "create", area, "--color", AREA_COLOR, "--force")

    projects = json.loads(
        gh.run("project", "list", "--owner", owner, "--format", "json", read_only=True)
    )
    project = next((p for p in projects["projects"] if p["title"] == PROJECT_TITLE), None)
    if project is None and not gh.dry_run:
        created = gh.run(
            "project", "create", "--owner", owner, "--title", PROJECT_TITLE, "--format", "json"
        )
        project = json.loads(created)
        try:
            gh.run(
                "project",
                "link",
                str(project["number"]),
                "--owner",
                owner,
                "--repo",
                slug.split("/")[1],
            )
        except RuntimeError as error:  # older gh versions have no `project link`
            print(f"  could not link the project to the repository: {error}")
    number = str(project["number"]) if project else "<new project>"
    todo = _todo_option(gh, number, owner)  # new cards go to the "Todo" column

    existing = json.loads(
        gh.run(
            "issue",
            "list",
            "--state",
            "all",
            "--limit",
            "500",
            "--json",
            "title,url",
            read_only=True,
        )
    )
    urls = {issue["title"].split(" · ")[0]: issue["url"] for issue in existing}
    for task in tasks:
        if task.key in urls:
            print(f"  {task.key} already exists: {urls[task.key]}")
            continue
        label_args = [arg for label in task.labels for arg in ("--label", label)]
        url = gh.run(
            "issue",
            "create",
            "--title",
            task.issue_title,
            "--body",
            task.body,
            "--milestone",
            sprint,
            *label_args,
        ).strip()
        added = gh.run(
            "project",
            "item-add",
            number,
            "--owner",
            owner,
            "--url",
            url or "<issue url>",
            "--format",
            "json",
        )
        if added and todo is not None:
            gh.run(
                "project",
                "item-edit",
                "--id",
                json.loads(added)["id"],
                "--project-id",
                project["id"],
                "--field-id",
                todo[0],
                "--single-select-option-id",
                todo[1],
            )
        print(f"  {task.key} created: {url}")


def _todo_option(gh: GitHub, number: str, owner: str) -> tuple[str, str] | None:
    """Return the ids of the project's Status field and of its "Todo" option, if any."""
    if gh.dry_run and number == "<new project>":
        return None
    fields = json.loads(
        gh.run(
            "project", "field-list", number, "--owner", owner, "--format", "json", read_only=True
        )
    )
    for field in fields.get("fields", []):
        if field.get("name") == "Status":
            for option in field.get("options", []):
                if option.get("name") == "Todo":
                    return field["id"], option["id"]
    return None


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("backlog", type=Path, help="path to a sprint backlog file")
    parser.add_argument("--dry-run", action="store_true", help="print the gh commands only")
    args = parser.parse_args(argv)
    try:
        sync(args.backlog, GitHub(args.dry_run))
    except (OSError, RuntimeError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
