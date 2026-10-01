# Open Source Stacks — Design Specification

**Product:** LFX Insights (public site) + LFX Self-Serve (org lens)
**Status:** Draft for engineering handoff
**Date:** 14 September 2026
**Source:** "LFX Insights | Open Source Stacks" prototype

---

## 0. How to read this document

This is a design spec, not a technical one. It describes what a person sees, what changes for them, and what state the interface should be in at every point. There is no data model, no API shape, no component naming.

Every screen in this document exists today except where marked **NEW**. The feature is deliberately additive: nothing in the current Insights experience is removed, and nothing an anonymous visitor sees today disappears.

---

## 1. What an Open Source stack is

An **Open Source stack** is a public, curated list of the open source projects an organization actually builds on, published by that organization and shown on LFX Insights.

Three things make it different from a collection:

|                    | Collection                      | Open Source stack                                |
| ------------------ | ------------------------------- | ------------------------------------------------ |
| Who makes it       | Anyone with an Insights account | An organization, via a verified employee         |
| What it claims     | "These projects are related"    | "_We_ depend on these projects"                  |
| Where it's created | Inside Insights                 | Inside LFX Self-Serve, by publishing a workspace |

Mechanically a stack **is** a collection — it renders on the same collection detail page, appears in the same Collections area, and behaves the same way for a reader. What it adds is _attribution_: a stack always belongs to a named organization, and that organization is accountable for it.

### 1.1 Terminology — locked

Use **"Open Source stack"** everywhere, in exactly this casing. Singular for one, "Open Source stacks" for many.

Do **not** use, anywhere in UI copy, alt text, tooltips, empty states, dialogs, toasts or menu items:

- "tech stack"
- "technology stack"
- "OSS stack"
- "stack" alone as a noun

The prototype went through an explicit rename pass from "tech stack" → "Open Source stack". Any string that survived is a bug.

### 1.2 Who can create one

A stack is created by someone acting on behalf of an organization, through the organization's lens in LFX Self-Serve. In practice: an employee whose email domain is verified against that organization.

This is not a permissions footnote — it is a **trust feature and should be surfaced as one**. The reason a reader can believe "Red Hat runs on these projects" is that only Red Hat can say it. The create-flow dialog (§6) presents verified-employee gating as a benefit, not a barrier.

---

## 2. The two halves of the feature

The feature spans both products, and the seam between them is the riskiest part of the design.

```
LFX INSIGHTS (public, read)          LFX SELF-SERVE (org lens, write)
─────────────────────────────        ─────────────────────────────────
Discover stacks                      Create a workspace
Read a stack                   ←──   Publish it as a public stack
Prompt to create one           ──→   Edit / unpublish it
Owner shortcut to manage       ──→   Workspace settings
```

**The core journey:** a person on Insights sees the shape of the feature, is handed to Self-Serve to actually make one, and lands back on Insights to see the result. Every hand-off must be signposted, seeded with context, and reversible.

**The cardinal rule:** Self-Serve must never be a dead end. Any entry point from Insights carries a visible way back (§7.4).

---

## 3. Information architecture and navigation

### 3.1 Global navigation — unchanged in shape

The Insights top bar keeps its current items:

`LFX Insights` · search · **Collections** · **Leaderboards** · **Open Source Index** · saved · avatar

**No new top-level nav item is added.** Stacks are a kind of collection and are found where collections are found. Adding a sixth destination would split discovery for no benefit and would imply stacks are a separate product surface.

### 3.2 Collections landing — new segment **NEW**

The Collections landing page today has a search field and a segmented tab row:

`Linux Foundation` · `Community` · `My collections`

It becomes:

`Linux Foundation` · `Community` · **`Open Source stacks`** · `My collections`

Placement rationale: `Open Source stacks` sits third, before `My collections`, because the first three are _browse_ tabs (things other people made) and the last is a _personal_ tab (things you made). Dropping stacks after "My collections" would read as a personal destination, which it isn't.

Notes:

- Search, sort and filter behave exactly as they do on the other tabs — same controls, same options, same placement. The stacks tab introduces no sorting or filtering of its own (nothing by industry, organization size, or stack size). Search above the tab row searches within the active tab.
- The tab is present for signed-out visitors. Stacks are public.
- Tab ordering, labels and the search behaviour are otherwise unchanged.

### 3.3 Collection cards — tag rule

Cards in the Collections grid may carry a small tag. The rule is narrow and absolute:

- **Open Source stack collections show a tag.** Label: `Open Source stack`.
- **Every other collection shows no tag** — including the user's own collections under "My collections". A tag on a personal collection was tried and removed; it added noise and told the user something they already knew.

The tag is the only visual difference between a stack card and a collection card in the grid. Same card size, same layout, same hover.

---

## 4. Page-by-page changes

### 4.1 Collections landing

**Current state.** Search field, tab row, responsive card grid.

**Changes.**

1. New `Open Source stacks` tab (§3.2).
2. Cards in that tab carry the `Open Source stack` tag (§3.3).
3. Each stack card additionally shows its **owning organization** — logo and name — because the organization is the point. Without it a stack card is indistinguishable from a curated collection.
4. New empty states (§8.1).

**The create affordance.** A button that starts the create flow appears:

- in the `Open Source stacks` tab **empty state**, and
- in the `My collections` tab **empty state**.

Label: `Create an Open Source stack`. This opens the intro dialog (§6) — it does **not** navigate straight to Self-Serve. A silent cross-app redirect from a button labelled "create" is the single worst possible outcome here; the user would land in a different product with no idea why.

An earlier label, "Create your organization's Open Source stack", was tried and reverted for length. Keep the short form.

---

### 4.2 Collection detail page — when the collection is a stack

**Current page structure** (unchanged, for reference):

- Back link: `All collections`
- Large serif title, with like count and `Share`
- Byline: `Updated 19 Mar 2026`
- Description
- Inline stats row: `Projects` · `Repositories` · `Contributors` · `Avg. Health`
- Toggle: `Only Linux Foundation projects`
- `Projects` section — three-per-row card grid
- Footer: `Looking for a project that's not listed? Submit project`

**Changes when the collection is an Open Source stack:**

| Element                    | Change                                                                                                                                                                   |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Back link                  | Label is unchanged — `All collections` — but it returns to the Collections landing with the `Open Source stacks` tab active, so the user lands back where they came from |
| Title area                 | `Open Source stack` tag sits with the title                                                                                                                              |
| Byline                     | Adds the owning organization — logo + name, linking to the organization details page                                                                                     |
| Action row — **non-owner** | Unchanged: like, Share. Likes behave exactly as they do on any other collection — same control, same count, same effect on ranking. No stack-specific treatment.         |
| Action row — **owner**     | **The like control is replaced by `Manage Open Source stack`** (settings icon). Share remains.                                                                           |
| Everything else            | Unchanged — stats, LF-only toggle, project grid, submit-project footer                                                                                                   |

**Why the like button is replaced, not joined.** An owner liking their own stack is meaningless, and the action row has no room for a third control at 1280px without wrapping. Replacement keeps the row balanced and makes the owner state legible at a glance.

**What `Manage Open Source stack` does:**

1. Switches the user from Insights to LFX Self-Serve.
2. Lands on the **Projects** page of the org lens.
3. Sets the active workspace to **this stack's own workspace** — not whatever workspace was last selected.
4. Opens that workspace's **settings dialog**, pre-filled with the stack's current name and description.

Points 3 and 4 are not optional polish. Landing a user in Self-Serve on an unrelated workspace, or on a blank dialog, breaks the promise the button made.

---

### 4.3 Organization details page

This is where the feature is most visible to a reader, and where the biggest structural change lands.

**Current state.** The organization page's right column carried a single operational stack, rendered as a long list of projects.

**New structure.** The right column becomes a **stacked list of Open Source stack cards**, because an organization can publish more than one. Red Hat might have a general "Red Hat Open Source stack", plus "Ansible automation stack", plus "AI inference stack".

**Section:**

- Header: `Open Source stacks`
- One card per published stack, stacked vertically
- No count in the header — it was tried and removed as redundant with the visible list
- **No cap on the number of stacks** an organization can publish
- **Ordered by recency** — most recently published or updated first. The list is not manually orderable, and does not sort by size. The organization's newest thinking sits at the top of the column, which is also the cheapest ordering to keep correct as stacks change.

**Each card contains, and only contains:**

1. Stack **name** — the whole card links to the stack's collection detail page
2. **Short description** — one line, truncated
3. **Overlapping project logos** — a compact avatar cluster
4. **Project count** — e.g. `24 projects`

The card was deliberately simplified. The previous design listed ~10 projects per stack; with multiple stacks that produced an unreadable column. The logo cluster plus a count carries the same signal in a fraction of the height, and the collection page is one click away for anyone who wants the full list.

**Owner affordance.** At the **end of the list**, below the last card:

- `Manage Open Source stacks`
- **Borderless** — no button chrome
- **Blue text and blue icon**
- Links to the **LFX Self-Serve Projects page**

Shown only to the owner. Not shown to other viewers. The settings icon that originally accompanied it was removed — text and the link icon only.

**Empty states:** §8.3.

---

### 4.4 Project details page

**New section: which organizations run this project.** **NEW**

On the project's **Overview** tab, in the **left column**, **below the health scorecard**:

- Section header: **`Organizations with this project in their Open Source stacks`**
  - **Always plural.** The header does not change wording when only one organization is listed, and carries no count. One rule, one string, no pluralization logic and no "1 organization" awkwardness.
- **Card grid, three per row**
- Each card: **organization logo**, **organization name**, **description**, **number of employees**

This is the reverse view of a stack and arguably the most valuable thing the feature produces: a maintainer can see who depends on their project, and an evaluator can see who else trusts it.

Cards link to the organization details page.

**Empty state:** §8.4. Expect this to be the most common state at launch — almost no project will have organizations pointing at it on day one, and the empty state must not make the project look neglected.

---

### 4.5 Explore / home page

No changes. Hero, leaderboards, the Open Source Index panel and the curated/community collection rails are untouched by this feature.

---

## 5. Owner vs. non-owner — complete matrix

"Owner" means the signed-in user can **administer this stack's workspace** in the organization's Self-Serve lens. Not merely a verified employee — an employee without workspace rights is a non-owner, and so is anyone signed out or unaffiliated.

**Insights does not own this definition.** It reads whatever the organization lens already says about the user's rights; it does not maintain a parallel notion of ownership, and it never grants access by linking. A user who follows an owner affordance into Self-Serve is subject to the org lens permissions mechanism on arrival, exactly as if they had navigated there directly — that mechanism decides what they can see and do, including showing them a restricted or read-only view. The link is a shortcut, never an authorization.

| Surface                           | Non-owner sees                                          | Owner additionally sees                         |
| --------------------------------- | ------------------------------------------------------- | ----------------------------------------------- |
| Collections landing — stacks tab  | Stack cards with `Open Source stack` tag and owning org | Nothing extra in the grid                       |
| Collections landing — empty state | Explanatory empty state, no create button               | `Create an Open Source stack` button            |
| Collection detail (stack)         | Tag, owning org, like, Share                            | `Manage Open Source stack` **in place of** like |
| Organization page — with stacks   | Stack card list                                         | `Manage Open Source stacks` link below the list |
| Organization page — no stacks     | Quiet empty message                                     | Empty state with a create prompt                |
| Project page — org cards          | Org cards                                               | Nothing extra                                   |

**Non-negotiable:** owner-only affordances never appear for a signed-out visitor, and never appear in a state where the click would fail. If the user cannot administer the workspace, the control is absent — not present-and-disabled. A disabled control invites a support ticket; an absent one asks nothing.

**Design/QA note:** the prototype exposes owner state as a switch so both variants of every screen can be reviewed side by side. Both must be designed, both must be reviewed.

---

## 6. The create flow — intro dialog

### 6.1 Why a dialog exists at all

Creating a stack means: leave Insights → go to LFX Self-Serve → find the org lens → make a workspace → add projects → make it public. That is not a trivial or self-evident process, and no button label can carry it.

The dialog exists to do three things before the user is handed across:

1. Say what an Open Source stack is, in plain language
2. Say who is allowed to create one
3. Say that they are about to be taken to the organization lens in LFX, and roughly what happens there

### 6.2 Tone

**Marketing-ish.** Benefit-led, warm, confident. This is a moment where the product is asking someone to do real work in another application; the case has to be made, not merely the instructions given.

Not: technical, procedural, or apologetic. No mention of workspaces-as-a-concept, permissions models, or data pipelines.

### 6.3 Structure — final

Top to bottom:

1. **Icon** — centred, above the title
2. **Title** — `Show the world what {Organization} is built on`
3. **Intro paragraph** — benefit-led; why publishing a stack is worth the org's time
4. **`Three steps, a few minutes`** — a short list setting expectations for what happens in Self-Serve
5. **Verified-employee note** — framed as a trust signal: only verified employees can publish, which is exactly why anyone believes the result
6. **CTA** — `Create your Open Source stack`
   - **Full width** of the dialog
   - **Insights blue**
   - **Open-in-new-tab / external-link icon**

**Explicitly removed during design — do not reintroduce:**

- The `New on LFX Insights` eyebrow above the title
- A `Maybe later` secondary button

Dismissal is via the dialog's close control and overlay click only. A single full-width CTA makes the intended path unambiguous; a paired secondary button gave "not now" equal visual weight to the thing we're asking for.

The external-link icon is load-bearing: it is the pre-click promise that the user is leaving this page.

### 6.4 Placement and triggers

The dialog is a **page-level overlay**. It must be able to sit over any screen, because its triggers are spread across the product.

Triggers:

- `Create an Open Source stack` in the Collections empty states (§4.1)
- The create prompt in the organization page empty state, owner view (§8.3)
- Any future create affordance

**Every** create affordance routes through this dialog. There is no path that jumps straight to Self-Serve. (In the prototype the Collections-page button was initially unwired and went nowhere — worth an explicit QA pass on each trigger.)

---

## 7. LFX Self-Serve — the hand-off points

Scoped to where the two products touch. The broader Self-Serve projects/workspace experience is out of scope here.

### 7.1 Where the user lands

The **Projects page** in the organization lens. Not a dashboard, not a settings root — the page where workspaces live, because a workspace is what becomes a stack.

From the Insights _manage_ entry point (§4.2), the correct workspace is already selected and its settings dialog is already open and populated.

From the Insights _create_ entry point (§6), the user arrives at the Projects page ready to create a new workspace.

In both cases the org lens applies its own permissions on arrival (§5). Arriving via a link from Insights confers nothing; if the user lacks rights, the lens handles that in its established way and Insights adds no special-case screen.

### 7.2 The concept bridge

The thing a user manipulates in Self-Serve is a **workspace** — a named list of projects. Making that workspace public is what publishes it to Insights as an Open Source stack.

This mapping is the single hardest idea in the feature. Two different words for two views of one object, in two different applications. The interface must say so where the switch happens — in the workspace settings dialog, at the public toggle — rather than assuming the user carried the concept across the app boundary.

### 7.3 Workspace settings dialog — required fields

When the **public Open Source stack** option is enabled, two fields become required:

- **Collection name**
- **Description**

Treatment while the requirement is unmet:

- **Red asterisk** on each label
- **Red border** on each field while empty
- **`Save` disabled** until both are filled

Rationale: these two fields are not internal metadata once the toggle is on — they are the title and the description of a public page carrying the organization's name. A stack published as "Workspace 2" with no description is worse for the organization than no stack at all. The validation is strict on purpose.

Turning the toggle back off releases the requirement.

### 7.4 The published workspace card

Once a workspace is published, its card on the Projects page shows:

**Header row:**

- Card **title**
- **`Published`** tag — green, immediately to the **right of the title**
- **Collection URL** link — inline, immediately after the tag
- **Three-dot menu** — far right of the header row

**Body:**

- Description
- **Divider**
- Project tags

**Three-dot menu contains:**

- `Manage Open Source stack`
- `Unpublish Open Source stack`

Both live in the menu. Neither is a standalone button on the card — that arrangement was tried and rejected for crowding the header. The collection URL sits to the **left** of the three-dot button.

Unpublished workspaces show no `Published` tag and no collection URL.

**What unpublishing does.** Unpublishing removes the stack from the public product entirely. The collection URL **404s** — it does not redirect to the organization page and does not survive as a private collection. Anyone holding a bookmark or a shared link gets a not-found page. The stack also disappears from the Collections landing, from the organization page, and from the organization cards on every project page it appeared on.

The workspace itself is untouched; only its public face is withdrawn. Because the effect is public and immediate, `Unpublish Open Source stack` must be confirmed before it takes effect, and the confirmation should say plainly that the public page will stop existing and its link will break.

### 7.5 Getting back to Insights

The **LF mark in the Self-Serve left rail** acts as a **"Back to LFX Insights"** affordance, clearing the cross-app context and returning the user to Insights.

This is the only guaranteed exit from the hand-off. Without it, a user pushed from Insights into Self-Serve has no signposted way home.

---

## 8. Empty states

Empty states carry more of this feature than usual, because at launch **almost every surface will be empty**. They are the primary marketing surface for stacks and must be designed accordingly, not treated as error states.

Principles applied throughout:

- Never blame the viewer for emptiness
- Never show a create prompt to someone who cannot create
- Distinguish "nothing exists yet" from "nothing matched your filter"

### 8.1 Collections landing — `Open Source stacks` tab, no stacks

- **Anyone:** explain what an Open Source stack is in one or two lines — this is many users' first encounter with the term.
- **Owner / anyone who could create one:** `Create an Open Source stack` button → intro dialog.
- **Signed-out:** explanation only, no create button.

### 8.2 Collections landing — `My collections`, nothing yet

Existing empty state, plus the `Create an Open Source stack` button.

### 8.3 Organization details page — no published stacks

Distinguish sharply:

- **Non-owner:** a quiet, neutral line — this organization hasn't published an Open Source stack. Small, low-contrast, no illustration, no call to action. A visitor to Red Hat's page should not be nagged about Red Hat's homework.
- **Owner:** a real prompt — this is the highest-intent moment in the whole feature. The person is looking at their own organization's page and can see the gap. Explain the value in a line, then offer `Create an Open Source stack` → intro dialog.

The section header may be hidden entirely in the non-owner empty case, to avoid an empty labelled region.

### 8.4 Project details page — no organizations

Expect this to be the default state at launch.

- Neutral, one line, low prominence
- **Must not imply the project is unused or unpopular** — it means no organization has _published a stack containing it yet_, which is a statement about stack adoption, not about the project
- No call to action: the viewer is almost never in a position to fix it

### 8.5 Collection detail — stack with no projects

Should be rare, since the Self-Serve flow requires projects in the workspace before publishing is useful, but must be handled:

- **Owner:** prompt to add projects, linking to Self-Serve
- **Non-owner:** neutral line, page otherwise intact

### 8.6 Search and filter results

Unchanged behaviour, but stack results must be representable in the existing search modal, tagged consistently with §3.3.

---

## 9. Review checklist

Per screen, both data states (populated / empty) and both permission states (owner / non-owner):

- [ ] Collections landing — Open Source stacks tab
- [ ] Collections landing — My collections tab
- [ ] Collection detail — stack
- [ ] Organization details page
- [ ] Project details page — Overview tab
- [ ] Intro dialog, from every trigger
- [ ] Self-Serve Projects page — arriving from _create_
- [ ] Self-Serve Projects page — arriving from _manage_ (correct workspace selected, dialog seeded)
- [ ] Workspace settings dialog — toggle off, toggle on and invalid, toggle on and valid
- [ ] Published workspace card, and its three-dot menu
- [ ] Return path to Insights from Self-Serve
