#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 by the Linux Foundation
#
# Test harness for noteren.py
#
# Run with:
#   pytest -v src/test/test_noteren.py
#
# The CLI tests build a throwaway bare git repo laid out like a single
# public-inbox shard (one blob named "m" per commit holding a raw email),
# so no real lore mirror is needed.

import json
import os
import runpy
import subprocess
import sys
from io import StringIO

import pytest

from crowdmail.errors import CommandExecutionError
from crowdmail.services.parse import noteren

HERE = os.path.dirname(os.path.abspath(__file__))
NOTEREN = os.path.join(HERE, "..", "crowdmail", "services", "parse", "noteren.py")
SEGMENT_ID = "11111111-1111-1111-1111-111111111111"
INTEGRATION_ID = "22222222-2222-2222-2222-222222222222"


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


def _git(gitdir, *args, stdin=None):
    out = subprocess.run(
        ["git", "--git-dir", str(gitdir)] + list(args),
        input=stdin,
        capture_output=True,
        check=True,
    )
    return out.stdout.decode().strip()


def _make_email(msgid, subject, frm, body, date="Mon, 1 Jan 2024 12:00:00 +0000"):
    hdr = f"From: {frm}\nSubject: {subject}\nDate: {date}\nMessage-ID: <{msgid}>\n\n"
    return (hdr + body).encode()


def _commit_email(gitdir, raw, parent=None):
    """Store raw email bytes as blob 'm' and return the new commit id."""
    blob = _git(gitdir, "hash-object", "-w", "--stdin", stdin=raw)
    tree = _git(gitdir, "mktree", stdin=f"100644 blob {blob}\tm\n".encode())
    args = ["commit-tree", tree, "-m", "msg"]
    if parent:
        args += ["-p", parent]
    env = dict(
        os.environ,
        GIT_AUTHOR_NAME="t",
        GIT_AUTHOR_EMAIL="t@t",
        GIT_COMMITTER_NAME="t",
        GIT_COMMITTER_EMAIL="t@t",
    )
    out = subprocess.run(
        ["git", "--git-dir", str(gitdir)] + args,
        stdout=subprocess.PIPE,
        check=True,
        env=env,
    )
    return out.stdout.decode().strip()


@pytest.fixture
def inbox_repo(tmp_path):
    """A bare git repo with two commits, each holding one raw email as 'm'."""
    gitdir = tmp_path / "0.git"
    subprocess.run(["git", "init", "--bare", "-q", str(gitdir)], check=True)

    m1 = _make_email(
        "msg1@example.com",
        "[PATCH] first",
        "Alice Example <alice@example.com>",
        "hello world\nsecond line\n",
    )
    m2 = _make_email(
        "msg2@example.com",
        "Re: [PATCH] first",
        "Bob Example <bob@example.com>",
        "> hello world\nlooks good to me\n",
    )

    c1 = _commit_email(gitdir, m1)
    c2 = _commit_email(gitdir, m2, parent=c1)
    _git(gitdir, "update-ref", "HEAD", c2)

    return {"gitdir": str(gitdir), "c1": c1, "c2": c2}


def _run_noteren(*args):
    return subprocess.run(
        [sys.executable, NOTEREN] + list(args),
        capture_output=True,
        text=True,
    )


def _std_args():
    return ["--segment-id", SEGMENT_ID, "--integration-id", INTEGRATION_ID]


# --------------------------------------------------------------------------
# Unit tests for helper functions
# --------------------------------------------------------------------------


def test_message_id_cleanup_strips_angles():
    assert noteren.message_id_cleanup("<abc@host>") == "abc@host"
    assert noteren.message_id_cleanup("  <abc@host>  ") == "abc@host"
    assert noteren.message_id_cleanup(None) == ""


def test_parse_author_name_and_email():
    name, email = noteren.parse_author("Alice Example <alice@example.com>")
    assert name == "Alice Example"
    assert email == "alice@example.com"


def test_parse_author_from_prefix_stripped():
    name, email = noteren.parse_author("From: Alice <alice@example.com>")
    assert name == "Alice"
    assert email == "alice@example.com"


def test_parse_author_bare_address():
    name, email = noteren.parse_author("alice@example.com")
    assert email == "alice@example.com"
    assert name == "alice@example.com"


def test_parse_author_obfuscated_at():
    _, email = noteren.parse_author("Alice <alice at example.com>")
    assert email == "alice@example.com"
    _, email = noteren.parse_author("Alice <alice(a)example.com>")
    assert email == "alice@example.com"


def test_get_body_raw_splits_on_blank_line():
    raw = b"From: a@b\nSubject: s\n\nline1\nline2\n"
    assert noteren.get_body_raw(raw) == "line1\nline2\n"


def test_run_command_captures_stdout_stderr():
    rc, out, err = noteren._run_command(
        [sys.executable, "-c", "import sys;print('ok');print('no', file=sys.stderr)"]
    )
    assert rc == 0
    assert out.strip() == b"ok"
    assert err.strip() == b"no"


def test_git_run_command_builds_args_with_gitdir(monkeypatch):
    seen = {}

    def fake_run(args, stdin=None):
        seen["args"] = args
        seen["stdin"] = stdin
        return 0, b"", b""

    monkeypatch.setattr(noteren, "_run_command", fake_run)
    noteren.git_run_command("/tmp/repo.git", ["status"], stdin=b"in")
    assert seen["args"] == ["git", "--git-dir", "/tmp/repo.git", "--no-pager", "status"]
    assert seen["stdin"] == b"in"


def test_git_run_command_builds_args_without_gitdir(monkeypatch):
    seen = {}

    def fake_run(args, stdin=None):
        seen["args"] = args
        return 0, b"", b""

    monkeypatch.setattr(noteren, "_run_command", fake_run)
    noteren.git_run_command(None, ["status"])
    assert seen["args"] == ["git", "--no-pager", "status"]


def test_get_body_prefers_first_non_html_text_part():
    msg = noteren.email.message.EmailMessage()
    msg.make_mixed()

    html_part = noteren.email.message.EmailMessage()
    html_part.set_content("<b>ignored</b>", subtype="html")

    plain_part = noteren.email.message.EmailMessage()
    plain_part.set_content("chosen text", subtype="plain")

    msg.attach(html_part)
    msg.attach(plain_part)

    assert noteren.get_body(msg).strip() == "chosen text"


def test_get_body_skips_text_part_with_no_payload():
    # A part that claims text/* but is actually a container: get_payload(decode=True)
    # returns None, so get_body() must skip it and pick the next text part.
    outer = noteren.email.message.EmailMessage()
    outer["Content-Type"] = "text/plain"
    inner = noteren.email.message.EmailMessage()
    inner.set_content("real body", subtype="plain")
    outer.set_payload([inner])

    assert noteren.get_body(outer).strip() == "real body"


def test_get_body_unknown_charset_falls_back_to_utf8():
    raw = (
        b"From: A <a@example.com>\n"
        b"Subject: s\n"
        b"Content-Type: text/plain; charset=x-unknown-charset\n"
        b"Content-Transfer-Encoding: 8bit\n"
        b"\n"
        b"hello \xe2\x98\x83\n"
    )
    msg = noteren.email.parser.BytesParser(
        policy=noteren.emlpolicy, _class=noteren.email.message.EmailMessage
    ).parsebytes(raw)
    assert "hello" in noteren.get_body(msg)


def test_get_email_from_git_success(monkeypatch):
    calls = []

    def fake_git_run(git_dir, args, stdin=None):
        calls.append((git_dir, args, stdin))
        if args[0] == "ls-tree":
            return 0, b"blob123\n", b""
        if args[0] == "cat-file":
            return 0, b"raw-message", b""
        raise AssertionError("unexpected git args")

    monkeypatch.setattr(noteren, "git_run_command", fake_git_run)
    msg, blob = noteren.get_email_from_git("/tmp/repo.git", "commit123")
    assert msg == b"raw-message"
    assert blob == "blob123"
    assert calls[0][1][0] == "ls-tree"
    assert calls[1][1][0] == "cat-file"


def test_get_email_from_git_missing_commit_raises(monkeypatch):
    monkeypatch.setattr(noteren, "git_run_command", lambda *args, **kwargs: (0, b"", b""))
    with pytest.raises(CommandExecutionError):
        noteren.get_email_from_git("/tmp/repo.git", "deadbeef")


def test_get_email_from_git_missing_blob_raises(monkeypatch):
    def fake_git_run(git_dir, args, stdin=None):
        if args[0] == "ls-tree":
            return 0, b"blob123\n", b""
        return 1, b"", b"missing"

    monkeypatch.setattr(noteren, "git_run_command", fake_git_run)
    with pytest.raises(CommandExecutionError):
        noteren.get_email_from_git("/tmp/repo.git", "deadbeef")


def _parse_email(raw, source, channel, git_id, blob_id):
    return noteren.parse_email(raw, source, channel, git_id, blob_id, SEGMENT_ID, INTEGRATION_ID)


def test_parse_email_extracts_attributes_and_reference():
    raw = (
        b"From: Alice Example <alice@example.com>\n"
        b"Subject: [PATCH] x\n"
        b"Date: Mon, 1 Jan 2024 12:00:00 +0000\n"
        b"Message-ID: <msg@example.com>\n"
        b"References: <a@example.com> <b@example.com>\n"
        b"\n"
        b"line one\n"
        b"> quoted line\n"
        b"On Tuesday someone wrote\n"
        b"line two\n"
    )
    parsed = _parse_email(raw, "src", "chan", "commit1", "blob1")
    data = parsed["activityData"]
    assert data["sourceId"] == "msg@example.com"
    assert data["attributes"]["references"] == "b@example.com"
    assert data["attributes"]["lines"] == 2
    assert data["attributes"]["source"] == "src"
    assert data["member"]["displayName"] == "Alice Example"
    assert data["timestamp"].endswith("Z")


def test_parse_email_first_line_from_overrides_author_once():
    raw = (
        b"From: Relay via B4 Relay <relay@example.com>\n"
        b"Subject: patch\n"
        b"Date: Mon, 1 Jan 2024 12:00:00 +0000\n"
        b"Message-ID: <msg@example.com>\n"
        b"\n"
        b"\n"
        b"From: Real Author <real@example.com>\n"
        b"content line\n"
        b"From: Not Override <later@example.com>\n"
    )
    parsed = _parse_email(raw, "src", "chan", "commit1", "blob1")
    data = parsed["activityData"]
    assert data["member"]["displayName"] == "Real Author"
    assert data["member"]["identities"][0]["value"] == "real@example.com"
    assert data["attributes"]["via-b4-relay"] is True


def test_parse_email_out_of_range_date_leaves_timestamp_empty():
    # Year 10000 parses with parsedate_tz() but datetime.fromtimestamp()
    # rejects it (datetime.MAXYEAR is 9999), exercising the except branch.
    raw = (
        b"From: A <a@example.com>\n"
        b"Subject: s\n"
        b"Date: Tue, 1 Jan 10000 00:00:00 +0000\n"
        b"Message-ID: <msg@example.com>\n"
        b"\n"
        b"line\n"
    )
    parsed = _parse_email(raw, "src", "chan", "commit1234567890", "blob1234567890")
    assert parsed["activityData"]["timestamp"] == ""


def test_parse_email_invalid_date_leaves_timestamp_empty():
    raw = (
        b"From: A <a@example.com>\n"
        b"Subject: s\n"
        b"Date: definitely-not-a-date\n"
        b"Message-ID: <msg@example.com>\n"
        b"\n"
        b"line\n"
    )
    parsed = _parse_email(raw, "src", "chan", "commit1", "blob1")
    assert parsed["activityData"]["timestamp"] == ""


def test_parse_email_falls_back_to_raw_body(monkeypatch):
    raw = b"From: A <a@example.com>\nSubject: s\nMessage-ID: <msg@example.com>\n\nline one\n"
    monkeypatch.setattr(noteren, "get_body", lambda msg: None)
    parsed = _parse_email(raw, "src", "chan", "commit1", "blob1")
    assert "line one\n" in parsed["activityData"]["body"]


def test_parse_email_handles_invalid_from_header(monkeypatch):
    class BadFromMessage:
        def get(self, key, default=None):
            if key == "From":
                raise ValueError("bad header")
            headers = {
                "Subject": "s",
                "Date": None,
                "Message-ID": "<msg@example.com>",
                "References": None,
            }
            return headers.get(key, default)

    class Parser:
        def parsebytes(self, message):
            return BadFromMessage()

    monkeypatch.setattr(noteren.email.parser, "BytesParser", lambda **kwargs: Parser())
    monkeypatch.setattr(noteren, "get_body", lambda msg: "line\n")
    parsed = _parse_email(b"irrelevant", "src", "chan", "commit1", "blob1")
    assert parsed["activityData"]["member"]["displayName"] == "Unknown"
    assert parsed["activityData"]["member"]["identities"][0]["value"] == "unknown@kernel.org"


def test_parse_id_writes_json(monkeypatch):
    monkeypatch.setattr(noteren, "get_email_from_git", lambda *args: (b"x", "blob"))
    monkeypatch.setattr(
        noteren,
        "parse_email",
        lambda *args: {"activityData": {"sourceId": "m"}, "type": "x"},
    )
    out = StringIO()
    noteren.parse_id("s", "c", "/tmp/repo.git", "commit1", SEGMENT_ID, INTEGRATION_ID, out)
    assert json.loads(out.getvalue())["activityData"]["sourceId"] == "m"


def test_parse_range_calls_parse_id_for_each_commit(monkeypatch):
    monkeypatch.setattr(noteren, "git_run_command", lambda *args, **kwargs: (0, b"c1\nc2\n", b""))
    seen = []
    monkeypatch.setattr(
        noteren,
        "parse_id",
        lambda source, channel, git_dir, commit_id, segment_id, integration_id, outfh: seen.append(
            commit_id
        ),
    )
    noteren.parse_range("s", "c", "/tmp/repo.git", "r", SEGMENT_ID, INTEGRATION_ID, StringIO())
    assert seen == ["c1", "c2"]


def test_parse_range_exits_on_git_failure(monkeypatch):
    monkeypatch.setattr(noteren, "git_run_command", lambda *args, **kwargs: (1, b"", b"bad"))
    with pytest.raises(SystemExit):
        noteren.parse_range(
            "s", "c", "/tmp/repo.git", "bad-range", SEGMENT_ID, INTEGRATION_ID, StringIO()
        )


def test_cmd_with_git_id_calls_parse_id(monkeypatch, tmp_path):
    out = tmp_path / "out.json"
    seen = {}
    monkeypatch.setattr(
        noteren,
        "parse_id",
        lambda source, channel, git_dir, git_id, segment_id, integration_id, outfh: seen.update(
            {
                "source": source,
                "channel": channel,
                "git_dir": git_dir,
                "git_id": git_id,
                "segment_id": segment_id,
                "integration_id": integration_id,
                "is_file": outfh.name.endswith("out.json"),
            }
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "noteren.py",
            "--source",
            "s",
            "--channel",
            "c",
            "--git-dir",
            "/tmp/repo.git",
            "--segment-id",
            SEGMENT_ID,
            "--integration-id",
            INTEGRATION_ID,
            "--git-id",
            "abc",
            "--output",
            str(out),
        ],
    )
    noteren.cmd()
    assert seen["source"] == "s"
    assert seen["channel"] == "c"
    assert seen["git_dir"] == "/tmp/repo.git"
    assert seen["git_id"] == "abc"
    assert seen["segment_id"] == SEGMENT_ID
    assert seen["integration_id"] == INTEGRATION_ID
    assert seen["is_file"] is True


def test_cmd_with_git_range_calls_parse_range(monkeypatch):
    seen = {}
    monkeypatch.setattr(
        noteren,
        "parse_range",
        lambda source, channel, git_dir, git_range, segment_id, integration_id, outfh: seen.update(
            {
                "source": source,
                "channel": channel,
                "git_dir": git_dir,
                "git_range": git_range,
            }
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "noteren.py",
            "--source",
            "s",
            "--channel",
            "c",
            "--git-dir",
            "/tmp/repo.git",
            "--segment-id",
            SEGMENT_ID,
            "--integration-id",
            INTEGRATION_ID,
            "--git-range",
            "a..b",
        ],
    )
    noteren.cmd()
    assert seen["git_range"] == "a..b"


def test_cmd_without_output_uses_stdout(monkeypatch):
    seen = {}
    monkeypatch.setattr(
        noteren,
        "parse_id",
        lambda source, channel, git_dir, git_id, segment_id, integration_id, outfh: seen.update(
            {"outfh": outfh}
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "noteren.py",
            "--source",
            "s",
            "--channel",
            "c",
            "--git-dir",
            "/tmp/repo.git",
            "--segment-id",
            SEGMENT_ID,
            "--integration-id",
            INTEGRATION_ID,
            "--git-id",
            "abc",
        ],
    )
    noteren.cmd()
    assert seen["outfh"] is sys.stdout


def test_cmd_verbose_enables_debug_logging(monkeypatch):
    monkeypatch.setattr(noteren, "parse_id", lambda *args, **kwargs: None)
    called = {}
    monkeypatch.setattr(
        noteren.logging,
        "basicConfig",
        lambda **kwargs: called.update(kwargs),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "noteren.py",
            "--source",
            "s",
            "--channel",
            "c",
            "--git-dir",
            "/tmp/repo.git",
            "--segment-id",
            SEGMENT_ID,
            "--integration-id",
            INTEGRATION_ID,
            "--git-id",
            "abc",
            "--verbose",
        ],
    )
    noteren.cmd()
    assert called.get("level") == noteren.logging.DEBUG


def test_cmd_requires_id_or_range(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "noteren.py",
            "--source",
            "s",
            "--channel",
            "c",
            "--git-dir",
            "/tmp/repo.git",
            "--segment-id",
            SEGMENT_ID,
            "--integration-id",
            INTEGRATION_ID,
        ],
    )
    with pytest.raises(SystemExit):
        noteren.cmd()


def test_cmd_rejects_both_id_and_range(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "noteren.py",
            "--source",
            "s",
            "--channel",
            "c",
            "--git-dir",
            "/tmp/repo.git",
            "--segment-id",
            SEGMENT_ID,
            "--integration-id",
            INTEGRATION_ID,
            "--git-id",
            "a",
            "--git-range",
            "a..b",
        ],
    )
    with pytest.raises(SystemExit):
        noteren.cmd()


# --------------------------------------------------------------------------
# CLI / integration tests
# --------------------------------------------------------------------------


def test_cli_requires_id_or_range(inbox_repo):
    r = _run_noteren("--source=s", "--channel=c", "--git-dir", inbox_repo["gitdir"], *_std_args())
    assert r.returncode != 0
    assert "--git-range or --git-id" in r.stderr


def test_cli_rejects_both_id_and_range(inbox_repo):
    r = _run_noteren(
        "--source=s",
        "--channel=c",
        "--git-dir",
        inbox_repo["gitdir"],
        *_std_args(),
        "--git-id",
        inbox_repo["c1"],
        "--git-range",
        f"{inbox_repo['c1']}..{inbox_repo['c2']}",
    )
    assert r.returncode != 0
    assert "only provide" in r.stderr


def test_cli_git_id_stdout(inbox_repo):
    r = _run_noteren(
        "--source=https://example.com/0.git",
        "--channel=test-list",
        "--git-dir",
        inbox_repo["gitdir"],
        *_std_args(),
        "--git-id",
        inbox_repo["c1"],
    )
    assert r.returncode == 0, r.stderr
    data = json.loads(r.stdout)
    ad = data["activityData"]
    assert ad["sourceId"] == "msg1@example.com"
    assert ad["title"] == "[PATCH] first"
    assert ad["channel"] == "test-list"
    assert ad["member"]["displayName"] == "Alice Example"
    assert ad["attributes"]["git_commit_id"] == inbox_repo["c1"]
    # two non-quoted, non-empty body lines
    assert ad["attributes"]["lines"] == 2
    assert data["segmentId"] == SEGMENT_ID
    assert data["integrationId"] == INTEGRATION_ID


def test_cli_output_file(inbox_repo, tmp_path):
    out = tmp_path / "out.json"
    r = _run_noteren(
        "--source=s",
        "--channel=c",
        "--git-dir",
        inbox_repo["gitdir"],
        *_std_args(),
        "--git-id",
        inbox_repo["c2"],
        "--output",
        str(out),
    )
    assert r.returncode == 0, r.stderr
    # JSON went to the file, not stdout
    assert r.stdout == ""
    data = json.loads(out.read_text())
    assert data["activityData"]["sourceId"] == "msg2@example.com"
    assert data["activityData"]["member"]["displayName"] == "Bob Example"


def test_cli_git_range(inbox_repo):
    r = _run_noteren(
        "--source=s",
        "--channel=c",
        "--git-dir",
        inbox_repo["gitdir"],
        *_std_args(),
        "--git-range",
        f"{inbox_repo['c1']}..{inbox_repo['c2']}",
    )
    assert r.returncode == 0, r.stderr
    # c1..c2 is a single commit (c2); output is one JSON document
    data = json.loads(r.stdout)
    assert data["activityData"]["sourceId"] == "msg2@example.com"


def test_cli_unknown_commit_fails(inbox_repo):
    r = _run_noteren(
        "--source=s",
        "--channel=c",
        "--git-dir",
        inbox_repo["gitdir"],
        *_std_args(),
        "--git-id",
        "0000000000000000000000000000000000000000",
    )
    assert r.returncode != 0


# --------------------------------------------------------------------------
# Additional edge-case unit tests
# --------------------------------------------------------------------------


def test_get_body_returns_none_when_no_text_parts():
    """A message that contains only text/html must cause get_body to return None."""
    msg = noteren.email.message.EmailMessage()
    msg["Content-Type"] = "multipart/alternative"
    html_part = noteren.email.message.EmailMessage()
    html_part.set_content("<b>ignored</b>", subtype="html")
    msg.attach(html_part)
    assert noteren.get_body(msg) is None


def test_get_body_raw_no_blank_line_returns_empty():
    """When the raw message has no blank-line separator the body must be empty."""
    raw = b"From: a@b\nSubject: s\nno blank line here\n"
    assert noteren.get_body_raw(raw) == ""


def test_get_body_raw_blank_body_after_headers():
    """When headers are present but the body is empty the result must be empty."""
    raw = b"From: a@b\nSubject: s\n\n"
    assert noteren.get_body_raw(raw) == ""


def test_run_command_returns_nonzero_exit_code():
    """_run_command must surface a non-zero return code from the child process."""
    rc, out, err = noteren._run_command([sys.executable, "-c", "import sys; sys.exit(42)"])
    assert rc == 42


def test_parse_email_no_message_id_gives_empty_source_id():
    """A message without a Message-ID header must yield sourceId == ''."""
    raw = b"From: A <a@example.com>\nSubject: s\nDate: Mon, 1 Jan 2024 12:00:00 +0000\n\nbody\n"
    parsed = _parse_email(raw, "src", "chan", "c1", "b1")
    assert parsed["activityData"]["sourceId"] == ""


def test_parse_email_no_subject_gives_none_title():
    """A message without a Subject header must yield title == None."""
    raw = (
        b"From: A <a@example.com>\n"
        b"Date: Mon, 1 Jan 2024 12:00:00 +0000\n"
        b"Message-ID: <m@example.com>\n"
        b"\n"
        b"body\n"
    )
    parsed = _parse_email(raw, "src", "chan", "c1", "b1")
    assert parsed["activityData"]["title"] is None


def test_parse_email_single_reference():
    """References with a single entry must set reference to that cleaned id."""
    raw = (
        b"From: A <a@example.com>\n"
        b"Subject: s\n"
        b"Date: Mon, 1 Jan 2024 12:00:00 +0000\n"
        b"Message-ID: <m@example.com>\n"
        b"References: <only@example.com>\n"
        b"\n"
        b"body\n"
    )
    parsed = _parse_email(raw, "src", "chan", "c1", "b1")
    assert parsed["activityData"]["attributes"]["references"] == "only@example.com"


def test_parse_email_body_all_quoted_lines_gives_zero_lines():
    """A body consisting entirely of quoted/On lines must produce lines==0 and empty body."""
    raw = (
        b"From: A <a@example.com>\n"
        b"Subject: s\n"
        b"Date: Mon, 1 Jan 2024 12:00:00 +0000\n"
        b"Message-ID: <m@example.com>\n"
        b"\n"
        b"> quoted line one\n"
        b"> quoted line two\n"
        b"On Tuesday Bob wrote:\n"
    )
    parsed = _parse_email(raw, "src", "chan", "c1", "b1")
    assert parsed["activityData"]["attributes"]["lines"] == 0
    assert parsed["activityData"]["body"] == ""


def test_get_body_accepts_text_markdown_part():
    """text/markdown is a non-html text/* subtype and must be returned by get_body."""
    raw = (
        b"From: A <a@example.com>\n"
        b"Subject: s\n"
        b"Content-Type: text/markdown\n"
        b"Content-Transfer-Encoding: 8bit\n"
        b"\n"
        b"# heading\n"
        b"some markdown\n"
    )
    msg = noteren.email.parser.BytesParser(
        policy=noteren.emlpolicy, _class=noteren.email.message.EmailMessage
    ).parsebytes(raw)
    body = noteren.get_body(msg)
    assert body is not None
    assert "some markdown" in body


def test_parse_author_strips_double_quotes_from_email():
    """Double-quote characters must be removed from the parsed email address."""
    _, addr = noteren.parse_author('Alice <"alice"@example.com>')
    assert '"' not in addr
    assert "alice" in addr


def test_parse_email_multipart_alternative_uses_text_plain():
    """A multipart/alternative message must have its text/plain body extracted."""
    raw = (
        b"From: Alice <alice@example.com>\n"
        b"Subject: multipart test\n"
        b"Date: Mon, 1 Jan 2024 12:00:00 +0000\n"
        b"Message-ID: <multi@example.com>\n"
        b"MIME-Version: 1.0\n"
        b'Content-Type: multipart/alternative; boundary="boundary"\n'
        b"\n"
        b"--boundary\n"
        b"Content-Type: text/plain; charset=utf-8\n"
        b"\n"
        b"plain text body\n"
        b"--boundary\n"
        b"Content-Type: text/html; charset=utf-8\n"
        b"\n"
        b"<b>html body</b>\n"
        b"--boundary--\n"
    )
    parsed = _parse_email(raw, "src", "chan", "c1", "b1")
    ad = parsed["activityData"]
    assert ad["sourceId"] == "multi@example.com"
    assert "plain text body" in ad["body"]
    assert "<b>" not in ad["body"]


def test_parse_email_url_contains_message_id():
    """The url field must embed the message-id."""
    raw = (
        b"From: A <a@example.com>\n"
        b"Subject: s\n"
        b"Date: Mon, 1 Jan 2024 12:00:00 +0000\n"
        b"Message-ID: <unique-id@example.com>\n"
        b"\n"
        b"body\n"
    )
    parsed = _parse_email(raw, "src", "chan", "c1", "b1")
    assert parsed["activityData"]["url"] == "https://lore.kernel.org/r/unique-id@example.com"


def test_parse_email_url_with_no_message_id():
    """When Message-ID is absent the url must end with a trailing slash and empty id."""
    raw = b"From: A <a@example.com>\nSubject: s\nDate: Mon, 1 Jan 2024 12:00:00 +0000\n\nbody\n"
    parsed = _parse_email(raw, "src", "chan", "c1", "b1")
    assert parsed["activityData"]["url"] == "https://lore.kernel.org/r/"


def test_parse_email_result_structure():
    """Top-level result dict must have the expected keys."""
    raw = (
        b"From: A <a@example.com>\n"
        b"Subject: s\n"
        b"Date: Mon, 1 Jan 2024 12:00:00 +0000\n"
        b"Message-ID: <m@example.com>\n"
        b"\n"
        b"body\n"
    )
    result = _parse_email(raw, "src", "chan", "c1", "b1")
    assert result["type"] == "create_and_process_activity_result"
    assert result["segmentId"] == SEGMENT_ID
    assert result["integrationId"] == INTEGRATION_ID
    assert "activityData" in result


def test_parse_email_identities_structure():
    """Each parsed email must produce exactly two identities (verified / unverified)."""
    raw = (
        b"From: A <a@example.com>\n"
        b"Subject: s\n"
        b"Date: Mon, 1 Jan 2024 12:00:00 +0000\n"
        b"Message-ID: <m@example.com>\n"
        b"\n"
        b"body\n"
    )
    result = _parse_email(raw, "src", "chan", "c1", "b1")
    identities = result["activityData"]["member"]["identities"]
    assert len(identities) == 2
    verified = [i for i in identities if i["verified"]]
    unverified = [i for i in identities if not i["verified"]]
    assert len(verified) == 1
    assert len(unverified) == 1
    assert verified[0]["type"] == "username"
    assert unverified[0]["type"] == "email"
    assert all(i["platform"] == "mailinglist" for i in identities)


def test_parse_email_via_b4_relay_false_by_default():
    """via-b4-relay must be False when the From header does not contain 'via B4 Relay'."""
    raw = (
        b"From: Alice <alice@example.com>\n"
        b"Subject: s\n"
        b"Date: Mon, 1 Jan 2024 12:00:00 +0000\n"
        b"Message-ID: <m@example.com>\n"
        b"\n"
        b"body\n"
    )
    result = _parse_email(raw, "src", "chan", "c1", "b1")
    assert result["activityData"]["attributes"]["via-b4-relay"] is False


def _c1_argv(inbox_repo):
    """Return sys.argv for processing the first commit in inbox_repo."""
    return [
        "noteren.py",
        "--source=https://example.com/0.git",
        "--channel=test-list",
        "--git-dir",
        inbox_repo["gitdir"],
        "--segment-id",
        SEGMENT_ID,
        "--integration-id",
        INTEGRATION_ID,
        "--git-id",
        inbox_repo["c1"],
    ]


def test_main_entry_point_via_subprocess(inbox_repo):
    """Running noteren.py as __main__ must work end-to-end."""
    r = subprocess.run(
        [sys.executable, NOTEREN] + _c1_argv(inbox_repo)[1:],
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stderr
    data = json.loads(r.stdout)
    assert data["activityData"]["sourceId"] == "msg1@example.com"


def test_main_guard_invokes_cmd(monkeypatch, inbox_repo):
    """The ``if __name__ == '__main__'`` guard must invoke cmd() when run directly."""
    monkeypatch.setattr(sys, "argv", _c1_argv(inbox_repo))
    captured = StringIO()
    monkeypatch.setattr(sys, "stdout", captured)
    runpy.run_path(NOTEREN, run_name="__main__")
    data = json.loads(captured.getvalue())
    assert data["activityData"]["sourceId"] == "msg1@example.com"
