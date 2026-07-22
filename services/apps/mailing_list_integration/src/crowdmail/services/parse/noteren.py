#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-2.0-or-later
# Copyright (C) 2020, 2025 by the Linux Foundation
#
# Some functions taken from the tool 'b4' to help reuse some
# git handling and email parsing logic
#

import argparse
import datetime
import email.charset
import email.message
import email.parser
import email.policy
import email.utils
import json
import logging
import re
import subprocess
import sys

from crowdmail.enums import ActivityType
from crowdmail.errors import CommandExecutionError

# We want to always be using utf-8
email.charset.add_charset("utf-8", None)

# Policy we use for dealing with mail messages
emlpolicy = email.policy.EmailPolicy(
    utf8=True,
    cte_type="8bit",
    max_line_length=None,
    message_factory=email.message.EmailMessage,
)


# run a command and take the output.
# Shamelessly taken from 'b4' by Konstantin Ryabitsev <konstantin@linuxfoundation.org>
def _run_command(cmdargs: list, stdin: bytes | None = None) -> tuple[int, bytes, bytes]:
    logging.debug(cmdargs)
    sp = subprocess.Popen(
        cmdargs, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE
    )
    (output, error) = sp.communicate(input=stdin)
    return sp.returncode, output, error


# Run a git command at the specified git directory location
# Again, taken from 'b4'
def git_run_command(
    gitdir: str | None,
    args: list,
    stdin: bytes | None = None,
) -> tuple[int, bytes, bytes]:
    if gitdir:
        args = ["git", "--git-dir", gitdir, "--no-pager"] + args
    else:
        args = ["git", "--no-pager"] + args
    return _run_command(args, stdin=stdin)


def get_email_from_git(git_dir: str, git_id: str) -> tuple[bytes, str]:
    git_blob_id = ""

    ecode, out, err = git_run_command(
        git_dir, ["ls-tree", "--object-only", "--end-of-options", git_id]
    )
    if ecode == 0 and len(out):
        git_blob_id = out.decode().rstrip()  # strip the trailing \n

    if git_blob_id == "":
        msg = f'git commit id {git_id} was not found in the git repo "{git_dir}"'
        logging.error(msg)
        raise CommandExecutionError(msg)

    logging.debug("git_blob_id =%s", git_blob_id)

    ecode, out, err = git_run_command(git_dir, ["cat-file", "blob", git_blob_id])
    if ecode == 0 and len(out):
        # Return the raw bytes; the email parser handles per-part charset
        # decoding itself, and decoding here would corrupt non-utf-8 parts.
        blob = out
    else:
        msg = f'Unable to retrieve git blob {git_blob_id} from the git repo "{git_dir}"'
        logging.error(msg)
        raise CommandExecutionError(msg)

    return blob, git_blob_id


# Message ids come to us with a leading < character and a trailing > character
# so just whack them off
def message_id_cleanup(messageid: str | None) -> str:
    if messageid is None:
        return ""
    return messageid.strip().lstrip("<").rstrip(">")


# Strongly based on get_payload() from 'b4' (i.e. almost copied directly...)
# email parsing is rough, charsets are crazy, so let's try to do our best here
def get_body(msg: email.message.EmailMessage) -> str:
    # walk until we find the first text/plain part
    mcharset = msg.get_content_charset()
    if not mcharset:
        mcharset = "utf-8"

    mbody = None
    for part in msg.walk():
        cte = part.get_content_type()
        logging.debug(cte)
        # Accept any text/* part (plain, markdown, x-patch, x-diff, ...)
        # except text/html, which would dump tag soup into the body.
        if not cte.startswith("text/") or cte == "text/html":
            continue
        payload = part.get_payload(decode=True)
        if payload is None:
            continue
        pcharset = part.get_content_charset()
        if not pcharset:
            pcharset = mcharset
        try:
            payload = payload.decode(pcharset, errors="replace")
            mcharset = pcharset
        except LookupError:
            # what kind of encoding is that?
            # Whatever, we'll use utf-8 and hope for the best
            payload = payload.decode("utf-8", errors="replace")
            part.set_param("charset", "utf-8")
            mcharset = "utf-8"
        mbody = payload
        break

    return mbody


# Sometimes python can't handle multi-part or signed or other complex message
# formats (or I'm just too dumb to properly parse them, see the get_body
# function for what "should work", so let's just take the raw message in string
# form and manually parse it "by hand" as we don't really need/want all of that
# much information from it
def get_body_raw(msg: bytes) -> str:
    # Chunk through the whole text until we find the first blank line which
    # means that the header is done, so then the rest is the "body" which is
    # all we will need.
    header_over = False
    mbody = ""
    for line in iter(msg.decode("utf-8", errors="replace").splitlines()):
        if header_over:
            mbody = mbody + line + "\n"
            continue

        if len(line) == 0:
            header_over = True

    return mbody


# Parse out a name/email from an email "From:" line or string
def parse_author(author: str) -> tuple[str, str]:
    author_name = ""
    author_email = ""

    # We may be handed a full "From: Name <addr>" line from the body, so
    # strip the leading "From: " up front; otherwise it leaks into both
    # name and email when there are no angle brackets to anchor the regex.
    author = str(author)
    if author.startswith("From: "):
        author = author[6:]

    author_pattern = re.compile(r"\s*(.*)\s*<(.*)>")
    match = author_pattern.match(author)
    if match:
        author_name, author_email = match.groups()
        author_name = author_name.rstrip().replace('"', "")

    # Sometimes there is no "<...>" in a From line (these are mailing lists...)
    # So just set the email to be whatever we originally started out with and
    # hope the cleanup in later steps below will result in a sane address.
    if author_email == "":
        author_email = author

    # Some crazy lists (i.e. accel-config) have their email addresses as "name
    # at intel.com", with a literal " at " in the thing, ugh.  If this is the
    # case, replace that with a real "@".  Due to that, we have to strip out
    # the \" character as well.  Other people try to just use "(a)" instead of
    # "@", so handle that.
    # why, just why...
    author_email = author_email.replace(" at ", "@")
    author_email = author_email.replace("(a)", "@")
    author_email = author_email.replace('"', "")

    # If we were never able to determine a text "string" for the name, just set
    # it to the email address, as many emails just have:
    #  From: foo@example.com
    # so we will default to that and hopefully we can dig it out of the
    # database later on based on the email address.
    if author_name == "":
        author_name = author_email

    return author_name, author_email


def parse_email(
    message: bytes,
    source: str,
    channel: str,
    git_id: str,
    blob_id: str,
    segment_id: str,
    integration_id: str,
) -> dict:
    # Create an email parser so we can dig everything out of this message
    # Do NOT use headersonly=True here, as that prevents the parser from
    # building the multipart structure, which we will properly walk later on
    # and pick out the text/plain part of the message.
    e = email.parser.BytesParser(policy=emlpolicy, _class=email.message.EmailMessage).parsebytes(
        message
    )

    logging.debug(e)

    # Grab the author of the message, and split it out by name and address
    # Sometimes a patch will have a different author name in the body of the
    # email, in the first line that says "From: " in it.  This does NOT catch
    # that at the moment, we do that later on below in parsing the body
    try:
        author = e.get("From", None)
    except ValueError:
        author = None
    if author is None:
        author = "Unknown <unknown@kernel.org>"
    author_name, author_email = parse_author(author)

    # The b4 tool provides a relay for sending patches to the list, if it is
    # used, the author_name will have "via B4 Relay" in the text, so let's look
    # for it here
    used_b4 = False
    if author_name.find("via B4 Relay") != -1:
        used_b4 = True

    # Grab the subject
    subject = e.get("Subject", None)

    # Grab the date and convert it to ISO format
    date = ""
    email_date = e.get("Date", None)
    if email_date is not None:
        date_tz = email.utils.parsedate_tz(email_date)
        if date_tz is not None:
            try:
                # mktime_tz() returns a POSIX (UTC) timestamp; convert it as UTC,
                # not local time, since we label the result with "Z" below.
                date_dt = datetime.datetime.fromtimestamp(
                    email.utils.mktime_tz(date_tz), tz=datetime.UTC
                )
                # strftime("%Y") doesn't zero-pad years < 1000 on some platforms
                # (e.g. "102" instead of "0102"), which downstream JS Date parsing
                # rejects as invalid. Reject implausible years outright instead
                # of emitting a malformed timestamp.
                if not 1970 <= date_dt.year <= 9999:
                    raise ValueError(f"implausible year {date_dt.year}")
                # add a fake set of microseconds just to make date parsers on the ingest side happier
                date = date_dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000000Z"
            except (ValueError, OverflowError):
                logging.warning(
                    "Unparseable Date header %r in %s, leaving timestamp empty",
                    email_date,
                    git_id[:12],
                )

    # Grab the message id for this message
    msgid = message_id_cleanup(e.get("Message-ID", None))

    # Grab only the "last" reference in the list of references for this email.
    # "References:" is a long list, trying to show everything else in the
    # thread, but only the last one should be needed for us.  Famous last
    # words...
    reference = ""
    references = e.get("References", None)
    if references is not None:
        r = references.split()
        if r:
            reference = message_id_cleanup(r[-1])

    # Fob the body parsing off to a complex function where we can endless tweak
    # the logic if needed
    body = get_body(e)
    if body is None:
        logging.warning(
            'Unable to parse the message body for git_id:blob_id %s:%s, handling it in "raw" text format',
            git_id[:12],
            blob_id[:12],
        )
        body = get_body_raw(message)

    # dig through the email body to count the lines and save off what was
    # written by this author (i.e. not quoted lines).  Also determine if a new
    # "From: " line is in the email or not.
    num_lines = 0
    json_body = ""
    seen_content = False
    # Skip quoted lines ">" and empty ones when counting lines of new content
    for line in iter(body.splitlines()):
        if not seen_content and len(line) != 0:
            # Only the very first non-empty line of the body may override
            # authorship (this is where git-format-patch / b4 put it).  A
            # "From: " appearing later in the body is just content (e.g. a
            # forwarded or pasted message) and must not hijack attribution.
            seen_content = True
            if line.find("From: ") == 0:
                author_name, author_email = parse_author(line)

        if line.find(">") != 0 and line.find("On ") != 0 and line.find("From: ") != 0:
            if len(line) != 0:
                num_lines += 1
                json_body += line
                json_body += "\n"

    # now turn it all into a lovely JSON structure

    # We need 2 identities, one "verified" and one "not verified" for the db to work with
    id1 = {
        "platform": "mailinglist",
        "value": author_email,
        "type": "username",
        "verified": True,
    }
    id2 = {
        "platform": "mailinglist",
        "value": author_email,
        "type": "email",
        "verified": False,
    }
    identities = [id1, id2]

    member = {
        "displayName": author_name,
        "identities": identities,
    }

    attributes = {
        # It can be whatever is needed (CM accepts anything inside attributes)
        "git_commit_id": git_id,
        "git_blob_id": blob_id,
        "lines": num_lines,
        "references": reference,
        "source": source,
        "via-b4-relay": used_b4,
    }

    activityData = {
        "timestamp": date,
        "sourceId": msgid,
        "platform": "mailinglist",
        "channel": channel,
        "title": subject,
        "body": json_body,
        "url": source.rstrip("/") + "/r/" + msgid,
        "isContribution": True,
        "type": ActivityType.MESSAGE,
        # Matches groupsio's Groupsio_GRID MESSAGE score (services/libs/integrations/src/integrations/groupsio/grid.ts)
        "score": 6,
        "attributes": attributes,
        "member": member,
    }

    result = {
        "type": "create_and_process_activity_result",
        "segmentId": segment_id,
        "integrationId": integration_id,
        "activityData": activityData,
    }

    return result


def parse_id(
    source: str,
    channel: str,
    git_dir: str,
    git_id: str,
    segment_id: str,
    integration_id: str,
    outfh,
):
    msg, blob_id = get_email_from_git(git_dir, git_id)
    output = parse_email(msg, source, channel, git_id, blob_id, segment_id, integration_id)

    print(json.dumps(output, indent=2), file=outfh)


def parse_range(
    source: str,
    channel: str,
    git_dir: str,
    git_range: str,
    segment_id: str,
    integration_id: str,
    outfh,
):
    ecode, out, err = git_run_command(
        git_dir, ["log", "--reverse", "--format=%H", "--end-of-options", git_range]
    )
    if ecode != 0:
        logging.error(
            'git log failed for range "%s" in "%s": %s',
            git_range,
            git_dir,
            err.decode(errors="replace").strip(),
        )
        sys.exit(1)

    for commit_id in out.decode().splitlines():
        parse_id(source, channel, git_dir, commit_id, segment_id, integration_id, outfh)


def cmd():
    parser = argparse.ArgumentParser(
        prog="noteren", description="Dig information out of lore messages"
    )
    parser.add_argument("--source", help="Git repo url", required=True)
    parser.add_argument("--channel", help="Mailing list name", required=True)
    parser.add_argument(
        "--git-dir", help="Git directory of the lore repo to dig out of", required=True
    )
    parser.add_argument(
        "--segment-id", help="CDP segment id to attach the activity to", required=True
    )
    parser.add_argument(
        "--integration-id", help="CDP integration id to attach the activity to", required=True
    )
    parser.add_argument(
        "--git-id",
        help="Specific git commit id to get the information from",
        required=False,
    )
    parser.add_argument(
        "--git-range", help="Git range to get the information from", required=False
    )
    parser.add_argument(
        "--output",
        help="File to write the JSON output to (default: stdout)",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--verbose",
        help="Show debugging information",
        required=False,
        default=None,
        action="store_true",
    )

    args = parser.parse_args()
    source = args.source
    channel = args.channel
    git_dir = args.git_dir
    git_id = args.git_id
    git_range = args.git_range
    segment_id = args.segment_id
    integration_id = args.integration_id

    if args.verbose is not None:
        logging.basicConfig(level=logging.DEBUG)

    if git_id is None and git_range is None:
        logging.error("--git-range or --git-id must be specified")
        sys.exit(1)
    if git_id is not None and git_range is not None:
        logging.error("only provide --git-range OR --git-id")
        sys.exit(1)

    if args.output is not None:
        outfh = open(args.output, "w", encoding="utf-8")
    else:
        outfh = sys.stdout

    try:
        if git_id is not None:
            parse_id(source, channel, git_dir, git_id, segment_id, integration_id, outfh)

        if git_range is not None:
            parse_range(source, channel, git_dir, git_range, segment_id, integration_id, outfh)
    finally:
        if outfh is not sys.stdout:
            outfh.close()


if __name__ == "__main__":
    cmd()
