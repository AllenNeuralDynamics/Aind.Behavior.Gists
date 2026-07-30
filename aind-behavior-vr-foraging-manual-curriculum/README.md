# Manual VR Foraging curriculum → Dataverse

A gist for **manually injecting a curriculum suggestion into Dataverse**, overriding whatever the
rig last pushed for a subject. Useful for iterating on a curriculum and pushing it *posthoc*.

> At the rig, `Aind.Behavior.VrForaging` runs the curriculum and pushes suggestions to Dataverse
> automatically at the end of every session. Sometimes, though, we want to iterate on a curriculum
> offline and push the result ourselves — this gist shows how.

## Background

- [`Aind.Behavior.VrForaging`](https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging) —
  the task DSL and the curriculum DSL.
- [`clabe`](https://github.com/AllenNeuralDynamics/clabe/) — orchestrates acquisition and, at the
  end of each session, runs the curriculum and pushes it to Dataverse.
  - curriculum app: [`clabe/apps/_curriculum.py`](https://github.com/AllenNeuralDynamics/clabe/blob/main/src/clabe/apps/_curriculum.py)
  - Dataverse picker: [`clabe/pickers/dataverse.py`](https://github.com/AllenNeuralDynamics/clabe/blob/main/src/clabe/pickers/dataverse.py)

### The normal rig loop

```text
session start ─▶ picker fetches the recommended settings for the subject from Dataverse
             ─▶ session runs
             ─▶ curriculum app computes the next suggestion
             ─▶ picker pushes the new suggestion to Dataverse
             ─▶ (next day) repeat
```

## How the override works

The mechanism is simpler than it looks. In
[`clabe.pickers.dataverse`](https://github.com/AllenNeuralDynamics/clabe/blob/main/src/clabe/pickers/dataverse.py):

- Suggestions live in the Dataverse table `aibs_fact_mouse_proposed_behavior_sessionses`, keyed to a
  subject in `aibs_dim_mices` (via `aibs_mouse_id`).
- The table is **append-only**. Each row stores a serialized `TrainerState` in `aibs_trainer_state`,
  plus `aibs_task_name` and `aibs_stage_name`.
- At session start the picker reads back **only the most recent row**
  (`order_by="createdon desc", top=1`).

**So "overriding" a suggestion just means appending a newer row with our own `TrainerState`.** The
next session for that subject reads our row instead of the rig's. We never delete or edit the rig's
row.

```text
rig's last row  ─┐
                 ├─▶  we append a newer row  ─▶  next session picks up ours
our new row  ────┘        (append-only)
```

## What we push: a `TrainerState`

The payload is an `aind_behavior_curriculum.TrainerState`:

```python
TrainerState(
    curriculum=<Curriculum>,   # our custom curriculum (defined locally, see below)
    stage=<Stage>,             # the stage / task params to run next
    is_on_curriculum=True,
    active_policies=[...],      # policies active for that stage
)
```

A `Curriculum` is a graph of `Stage`s (each wrapping a `Task` + `TaskParameters`) connected by
`StageTransition`s, with `Policy`/`PolicyTransition`s inside each stage. See
`aind_behavior_curriculum` and its `example_project/curriculum.py` for the DSL.

## Design decisions

- **Curriculum** — custom curriculum defined **locally in this gist** ([`curriculum.py`](curriculum.py)).
  First-draft / simple; see *The curriculum* below.
- **Authentication** — explicit credentials from a secrets file / env vars (**not** KeePass).
- **Scope** — **batch of subjects**: push an override for each subject in a list.

The override plumbing (auth + push) is deliberately decoupled from the curriculum, so the curriculum
module can be iterated on independently. See [`plan.md`](plan.md) for the full design.

## The curriculum (`curriculum.py`)

`build_trainer_state()` **ignores any existing trainer state** and builds a fresh one starting
from the **real graduated stage of the [`LearningSets`](https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging/tree/main/src/packages/aind_behavior_vr_foraging_curricula/src/aind_behavior_vr_foraging_curricula/learning_sets)
curriculum** (imported, not re-implemented). The odor-pair generation is kept as-is —
`get_odor_sequence` still yields `(negative, positive)` odor-index pairs under the same rules
(no odor reappears in the previous pair). Two rules are added on top:

1. **Fixed sites per pair** — each pair contributes a fixed `REWARDED_PER_PAIR` (Y) rewarded
   sites + `NON_REWARDED_PER_PAIR` (Z) non-rewarded sites, shuffled together (both
   parameterized). Replaces the graduated stage's ramped 5+5 split.
2. **Contrast alternates pair-to-pair** — consecutive pairs alternate the visual contrast
   (the "context") between two levels: pair 0 → A, pair 1 → B, pair 2 → A, … On top of the
   rewarded / non-rewarded odor discrimination, the context flips every pair.

To express rule 2 in a `SequenceEnvironment` (a site's contrast lives in its patch definition,
and `patch_indices` only references patches by `state_index`), the 14 graduated patches are
**duplicated once per context** (N → N × `len(CONTEXT_CONTRASTS)` = 28): context A holds the
originals at contrast A, context B holds copies (`state_index += N`, `_ctx1` label) at contrast
B. The sequence references context-A patches on even pairs and context-B patches on odd pairs.
It all stays a **single block**.

The base comes from `aind_behavior_vr_foraging_curricula.learning_sets.stages.make_s_graduated`
— a `SequenceEnvironment` of single-reward-site patches over 7 odors. That package is a **git
dependency** (see `pyproject.toml`), so the learning sets (and their pair generator) are used
verbatim, not copied.

⚠️ **Review these:**

- The graduated stage's cross-session `start_policies` (`p_introduce_negative_sites`,
  `p_water_cap`) are **dropped** — this is a one-shot static injection, not a `Trainer` run,
  so we reuse only the stage's *task logic* and the pair generator, not its policy graph.
- Contrast intent was `-0.5 → 0.5`, but the schema constrains `RenderSpecification.contrast`
  to **[0, 1]**, so `CONTEXT_CONTRASTS` maps the two contexts onto that range — tune to taste.

Targets `aind-behavior-vr-foraging >= 1.2`, `aind-behavior-vr-foraging-curricula >= 1.2`
(git), and `aind-clabe[aind-services] >= 0.10.6`.

## Usage

```bash
uv sync

# credentials: copy dataverse.env.example -> ../secrets/dataverse and fill it in
#              (or export the DATAVERSE_* env vars)

# 1. Dry run first — builds + validates every trainer state, pushes nothing:
uv run main.py --subjects 828426 841306 --dry-run

# 2. Push for real (prompts for confirmation; --yes to skip):
uv run main.py --subjects-file subjects.txt

# Sanity-check the curriculum in isolation:
uv run curriculum.py
```

Files: [`curriculum.py`](curriculum.py) (what to push) · [`dataverse_push.py`](dataverse_push.py)
(auth + append) · [`main.py`](main.py) (batch CLI).

## Prior art in this repo

[`../update-quarantined-vr-foraging/main.py`](../update-quarantined-vr-foraging/main.py) performs the
full rig-style flow (build `DataversePicker`, set its session, run `CurriculumApp`, then
`picker.push_new_suggestion(...)`) against already-acquired session files. It is the best worked
example of the moving parts and is worth reading before this gist.

## Status

✅ Implemented and verified end-to-end via `--dry-run` (build + validate, no push).
[`curriculum.py`](curriculum.py) now builds on the **real** `LearningSets` graduated stage
(imported from `aind-behavior-vr-foraging-curricula`, a git dependency) rather than a stub.
The only not-yet-exercised path is the real push (needs live Dataverse credentials). See
[`plan.md`](plan.md) for the design and open questions.
