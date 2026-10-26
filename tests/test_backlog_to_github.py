import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "backlog_to_github.py"
spec = importlib.util.spec_from_file_location("backlog_to_github", SCRIPT)
backlog = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = backlog  # dataclasses look their module up here
spec.loader.exec_module(backlog)

BACKLOG = """# Sprint 1 backlog

**Sprint goal:** a goal.

## Close phase 1

### S1-01 · Rebuild the panel
- Must · 1 h · data
- Done when: the reports carry version 3.

### S1-02 · Set up the board
- Could · 0.5 h · devops
- Done when: the board exists.

**Total:** 2 tasks.
"""


def test_parse_backlog():
    sprint, tasks = backlog.parse_backlog(BACKLOG)

    assert sprint == "Sprint 1"
    assert [task.key for task in tasks] == ["S1-01", "S1-02"]
    first = tasks[0]
    assert first.issue_title == "S1-01 · Rebuild the panel"
    assert first.labels == ["priority: must", "data"]
    assert first.hours == 1.0
    assert first.body.startswith("- Done when: the reports carry version 3.")
    assert first.body.endswith("Priority: Must · Estimate: 1 h · Area: data")
    assert "Total" not in tasks[1].body  # text after the last task is not part of it


def test_parse_rejects_a_task_without_priority_line():
    with pytest.raises(ValueError, match="S1-01"):
        backlog.parse_backlog("# Sprint 1 backlog\n\n### S1-01 · Title\n- Done when: x\n")


class FakeGitHub:
    def __init__(self, existing_titles):
        self.dry_run = False
        self.calls = []
        self.existing = [
            {"title": title, "url": f"https://x/{i}"} for i, title in enumerate(existing_titles)
        ]

    def run(self, *args, read_only=False):
        self.calls.append(args)
        if args[:2] == ("repo", "view"):
            return json.dumps({"nameWithOwner": "me/repo", "owner": {"login": "me"}})
        if args[0] == "api" and "milestones?" in args[1]:
            return json.dumps([{"title": "Sprint 1"}])
        if args[:2] == ("project", "list"):
            projects = [{"title": backlog.PROJECT_TITLE, "number": 3, "id": "PVT_1"}]
            return json.dumps({"projects": projects})
        if args[:2] == ("project", "field-list"):
            status = {"name": "Status", "id": "F_1", "options": [{"name": "Todo", "id": "O_1"}]}
            return json.dumps({"fields": [{"name": "Title", "id": "F_0"}, status]})
        if args[:2] == ("project", "item-add"):
            return json.dumps({"id": "ITEM_9"})
        if args[:2] == ("issue", "list"):
            return json.dumps(self.existing)
        if args[:2] == ("issue", "create"):
            return "https://github.com/me/repo/issues/9\n"
        return ""


def test_sync_creates_only_missing_issues(tmp_path):
    path = tmp_path / "sprint-1-backlog.md"
    path.write_text(BACKLOG, encoding="utf-8")
    gh = FakeGitHub(existing_titles=["S1-01 · Rebuild the panel"])

    backlog.sync(path, gh)

    created = [call for call in gh.calls if call[:2] == ("issue", "create")]
    assert len(created) == 1
    assert "S1-02 · Set up the board" in created[0]
    added = [call for call in gh.calls if call[:2] == ("project", "item-add")]
    assert len(added) == 1
    assert "https://github.com/me/repo/issues/9" in added[0]
    edited = [call for call in gh.calls if call[:2] == ("project", "item-edit")]
    assert edited == [
        (
            "project",
            "item-edit",
            "--id",
            "ITEM_9",
            "--project-id",
            "PVT_1",
            "--field-id",
            "F_1",
            "--single-select-option-id",
            "O_1",
        )
    ]
    assert not [call for call in gh.calls if call[:2] == ("project", "create")]
