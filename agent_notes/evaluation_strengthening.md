# Evaluation Strengthening Assessment

## Context

After the config-only rerun, the portfolio will change. This note identifies
what should be added, removed, or modified in the evaluation to strengthen
the experiments, based on insights from the data wrangling audit.

---

## 1. REQUIRED changes (legacy cleanup after rerun)

### 1a. Remove legacy mentions

Current Section 5, lines 154-156:

> Parameterized batching uses the released configurations; the portfolio
> additionally retains two legacy local-search variants, one equivalent
> to a released configuration and the other using a previously supported
> random start.

After the rerun, no legacy variants exist. This sentence must be removed.

### 1b. Remove the 9.57% historical reference

Current Section 5, line 246:

> reducing the validation gap from 9.57% (sequential-only portfolio) to zero

After the rerun, there is no sequential-only counterfactual. The statement
should simply read: "RR-NF reproduces all 14,300 proven optima."

### 1c. Remove runtime qualification

Current appendix, lines 567-568:

> The reported SPRP-SS execution runtime corresponds to the original
> sequential portfolio and excludes the subsequently incorporated RR-NF
> result.

After the rerun, all pipelines (including RR-NF) will have runtimes.
This qualification must be removed.

---

## 2. HIGH-IMPACT additions

### 2a. Validation gap decomposition

**Problem**: The validation table (Table 3) shows the VBS gap to the
literature. The selection table (Table 4) shows the SBS regret to the VBS.
But the reader must mentally combine these to understand the total gap
from the SBS to the literature. More importantly, the decomposition tells
us *why* the gap exists:

- **Portfolio gap** (VBS to literature): the best available pipeline still
  can't match the reference → algorithm design is the bottleneck
- **Selection gap** (SBS to VBS): fixing one pipeline loses performance
  → algorithm selection is the answer

**Proposal**: Add a column "SBS-to-Ref [%]" to the validation table, or a
combined table that shows:

| Instance Set | VBS-to-Ref [%] | SBS-to-VBS [%] | SBS-to-Ref [%] |
|---|---|---|---|
| HennWaescher | 3.54 | 0.45 | ~3.98 |
| MuterOencan | 3.12 | 0.91 | ~4.02 |
| Foodmart | 6.30 | 1.75 | ~8.02 |

This decomposition directly shows that on Henn/Muter, the gap is mostly
portfolio (algorithm design), while on Foodmart, selection also matters.

**Implementation**: Already computable from existing metrics. Add a
`SBS-to-Ref` column to `generate_validation()` in
`generate_paper_outputs.py`.

### 2b. Batching-family-level component restriction

**Problem**: The current component restriction fixes "batching" to the
SBS's batching choice. But after the rerun, batching has 10+ configured
variants across 4 families. Fixing to one specific configuration tells us
that "batching matters" but not *which batching family matters*.

**Proposal**: Add a batching-family-level restriction analysis to the
appendix:

- Fix to Savings (any ClarkAndWright variant) → residual
- Fix to Local Search (any LS variant) → residual
- Fix to Seed (any SeedBatching variant) → residual
- Fix to Simple/Constructive (FiFo, OrdNr, DueDate, RAND) → residual

This tells us whether the portfolio gap is due to lacking a batching
*family* or just lacking the right *configuration* within a family.

**Example interpretation**: If fixing to LS leaves 0% residual but fixing
to Savings leaves 2% residual, then LS is the necessary family and the
specific LS configuration doesn't matter much. If both leave ~2% residual,
then no single batching family suffices and per-instance selection across
families is needed.

**Implementation**: Add a `batching_family` column to the result dataframe
(mapping each batching_algo to one of: Simple, Savings, Seed, LS, CBR).
Then compute family-restricted residuals in `components.py`.

### 2c. Routing-heuristic interaction in LS batching (Kris only)

**Problem**: On Kris, the rerun will include 5 LS batching variants
that differ in both routing heuristic (NN vs SShape) and start batching
(FiFo vs OrderNrFiFo vs DueDate). The current analysis treats all LS as
one "batching" choice. But the embedded routing heuristic in LS batching
creates a routing-batching interaction.

**Proposal**: In the appendix, for Kris, show which LS variant the VBS
prefers by instance characteristics. This doesn't require a new table —
the winner distribution already shows this. But the prose should note
whether the LS routing heuristic or the LS start batching drives the
difference.

**Implementation**: No new code; just read the winner distribution table
more carefully in the prose.

---

## 3. MEDIUM-IMPACT additions

### 3a. Per-stage runtime breakdown

**Problem**: The current runtime table reports only pipeline synthesis
and total execution. After the rerun, per-stage times are available in
the provenance (IA time, batching time, routing time, scheduling time).

**Proposal**: Add an appendix table showing mean per-stage execution time
by instance set. This reveals which stage dominates computational cost
(e.g., LS batching at 240s time limit vs. exact routing in seconds).

**Implementation**: The provenance data is already in the summary JSONs.
Add a `generate_stage_runtimes()` function to `generate_paper_outputs.py`.

### 3b. Oracle portfolio gain summary

**Problem**: The selection table shows SBS regret but doesn't summarize
the total oracle gain (VBS vs SBS) in a single number per set.

**Proposal**: Add a "VBS-SBS gap [%]" row or column that shows the
percentage improvement available from perfect per-instance selection:
`100 * (SBS_mean - VBS_mean) / SBS_mean`.

This is the same as the mean SBS regret expressed as a fraction of the
SBS mean, not the VBS. It's a different normalization that answers "how
much can selection improve?" rather than "how much does fixing lose?"

**Implementation**: Already computable; add to `selection_stats()` in
`metrics.py`.

### 3c. Loader runtime and layout-cache impact

**Problem**: The pipeline runtime table reports synthesis and execution
wall-clock per instance, but the one-time domain-loading cost (instance
parsing, layout building) is hidden inside the execution time.  After
the rerun, per-loader timing is available in the summary JSON, allowing
a decomposition into loading vs. solving and a quantification of the
layout cache benefit.

**Proposal**: Add an appendix table showing, per instance set:

| Instance Set | $n$ | Layout [s] | Instance [s] | Layout hit [\%] | Inst. hit [\%] | Savings [s] |
|---|---|---|---|---|---|---|

- **Layout [s]**: mean `layout_load_time` for cache misses (parse + build)
- **Instance [s]**: mean `instance_load_time` for cache misses (parse + build domain)
- **Layout hit [\%]**: share of pipelines that reused a cached layout
- **Inst. hit [\%]**: share of pipelines that reused a cached domain
- **Savings [s]**: estimated wall-clock avoided by caching
  ($n_\text{hits} \times t_\text{miss}$)

This separates the one-time loading cost (shared across pipelines) from
the per-pipeline solving cost and shows how much the layout cache saves
when many pipelines share the same warehouse layout.

**Implementation**: Done.  `LayoutLoader.run()` and `InstanceLoader.run()`
now write `.timing.json` sidecars with `parse_time`, `build_time`, and
`total_time`.  `_build_provenance_summary()` reads these and adds a
`loader_timing` block to the summary JSON.  `generate_loader_runtimes()`
in `generate_paper_outputs.py` produces the table from the df.  Loader
times are kept separate from `total_cpu_time` (algorithm stages only).

---

## 4. NOT recommended (out of scope)

### 4a. Instance-feature analysis

Grouping instances by features (n_orders, n_aisles) and showing regret
by group would explain *where* selection potential is concentrated. But
this adds substantial complexity and table space. The current exceedance
rates already show the tail behavior.

### 4b. Per-instance selector evaluation

Training a simple selector on instance features is a different paper
(algorithm selection). The current paper's contribution is the portfolio
synthesis, not the selector. The SBS regret and VBS gap quantify the
*opportunity* for selection without claiming to solve it.

---

## 5. Pipeline-space table corrections after rerun

After the RR-NF card fix and the rerun:

| Set | IA* | B* | R* | IAR* | BR* | S* | Full | Evaluated |
|---|---|---|---|---|---|---|---|---|
| SPRP | 1 | 1 | 7 | 0 | 0 | 0 | 7 | 6 |
| SPRP-SS | 5 | 1 | 7 | 1 | 0 | 0 | 36 | 31 |
| Bahceci | 1 | 11 | 7 | 0 | 1 | 0 | 78 | 61 |
| Henn | 1 | 11 | 7 | 0 | 1 | 0 | 78 | 60 |
| Muter | 1 | 11 | 7 | 0 | 1 | 0 | 78 | 60 |
| Foodmart | 1 | 9 | 6 | 0 | 1 | 0 | 55 | 40 |
| Kris | 1 | 15 | 6 | 0 | 0 | 3 | 288 | 300 |

Key changes from current table:
- SPRP: IAR*=0 (was 0, but now card-enforced, not manually corrected)
- SPRP-SS: IAR*=1 (scattered storage, card-admitted)
- Foodmart: Evaluated=40 (was 45; CBR excluded + legacy removed)
- Kris: Evaluated=300 (was 200; new LS variants added)

Note: B* includes SingleOrderBatching (template, not a card). R*
includes TSP in the "Full applicable" count but TSP is excluded from
the evaluated portfolio by design.

---

## 6. Recommended action plan

Before the rerun:
1. No code changes needed — the rerun scripts are correct.

After the rerun:
1. Remove legacy mentions, 9.57% reference, runtime qualification (Section 5
   + appendix)
2. Add validation gap decomposition (SBS-to-Ref column)
3. Add batching-family-level component restriction (appendix table)
4. Add per-stage runtime breakdown (appendix table)
5. Add loader runtime + cache impact table (appendix table) — implemented
6. Regenerate all tables from the clean config-only df
7. Update pipeline-space table with corrected counts
8. Update algorithm count if needed
