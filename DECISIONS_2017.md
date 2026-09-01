# Cleaning and construction decisions inventory

Goal: unify in one place all the cleaning and variable-construction decisions that appear in:
* code for 2017 report in `Colombia-BRT-IE` (currently and temporarily in `old-code/`)
* `data-clean-silver-from2016to2019.py` (current silver notebook in Databricks)
* code for 2017 analysis with a sample in Stata in `Colombia-BRT-IE` (currently and temporarily in `sample-code/)

Decide what to keep/change/discard in the new pipeline over **all** the data (bronze 2016–2019).

Verdict convention: ✅ keep | 🔧 adapt/change | ❓ decide (needs discussion or information) | ❌ discard. Verdicts already marked are proposals.

Principles:

- **Structural row-level cleaning** (parsing, casting, duplicates, fully-empty rows, excluding duplicate re-delivery files) runs over **all** of 2016–2019: these steps are local to each row, so incomplete months elsewhere cannot contaminate them.
- **Card-level, window-dependent constructs** (infrequent users, superswipers, `ever_*` profiles, profile imputation, subsidy pre/post flags) are a different story: they ARE contaminated by incomplete data (a card can look "infrequent" just because its 2018 trips are in missing/broken files). These are therefore computed **parameterized to the analysis window** (currently Oct 2016 – Sep 2017, which we verify complete), and recomputed if/when the window expands after mapping the broken/missing files.
- Different cleaning stages: we separate **Cleaning 1** (structural data quality: duplicates, empty rows — lives in the same notebook/table as types & formats, drops rows) from **Cleaning 2** (analysis-oriented and period-dependent cleaning: super swipers, infrequent users, implausible balances, impossible fares — implemented as *tags*, with the drop decided at analysis time). Cleaning 2 lives in a **separate notebook** whose first step is filtering to the analysis window, producing a window-restricted silver table (see pipeline structure below).
- Pre-aggregation variable construction happens in Spark. Stata is left for the final analysis only.
- Deep cleaning is focused on the main variables: timestamp, cardnumber, card profile (`account_name`), value. The secondary variables (balance, station, operator, emisor, line) are still cast and explicitly mapped in silver, and every old-code cleaning step that relies on them is kept: the implausible-balance filter, the duplicate definition, the profile imputation, the SITP station fix. **The only old-code cleaning we do NOT replicate is the standardization of station names and geographic attributes**, which needs old-code's station dictionaries (`station_fix_dict.csv`, `station_geo_dict.csv`, loaded in `old-code/generate_variables.py:10-16`) to be recovered from `Colombia-BRT-IE`.
- The card-level dataset is built for **all** cards (allows counting who ends up in/out of the sample). The monthly panel is exported only for cards with trips before and after the policy (`in_6m_bef == 1 & in_6m_aft == 1`).
- We do not benchmark against the 1% sample (we are not sure how it was built). The goal is a complete dataset we understand and trust.

## Proposed pipeline structure

One transaction-level clean **silver table**.

| Phase | Content | Where |
|---|---|---|
| 1a. Silver: types & formats | Dates, numeric casting, categorical normalization. Drops no rows. | Databricks, `data-clean-silver-from2016to2019.py` |
| 1b. Silver: Cleaning 1 | Structural data quality: duplicates, fully-empty rows, exclusion of duplicate re-delivery files. Drops objective errors. | Same notebook → same silver table |
| 1c. Silver: Cleaning 2 | First step: filter to the analysis window. Then analysis-oriented tags as columns: infrequent users, superswipers, implausible balances, impossible fares, early zeros. Tags only. | Other notebook → other silver table restricted to period of analysis |
| 2. Construction | Transaction-level variables: imputed profile, transfer, trips, subsidized fares, subsidy flags. | Other notebook in Databricks - other table |
| 3. Aggregation | Card-level dataset (all cards) + card×month panel (balanced). | Other notebook in Databricks - other table  |
| 4. Export & analysis | Download (or not) the restricted panel; merge Sisbén III at the person level (outside Databricks); analysis in Stata. | Outside Databricks |

---

## A. Phase 1a — Silver: types and formats

| # | Step | Source | Verdict |
|---|---|---|---|
| A1 | Ingestion diagnostics: control table vs bronze, weird encoding in dates, ingestion duplicates | current silver, section 3 | ✅ written and run for Oct 2016 - Sep 2017 (no issues there, but to solve issues for later dates)|
| A2 | Parsing of `fecha_transaccion` (string→date+timestamp, multi-format) and `clearing_date` (date), preserving original strings | current silver, section 4 | ✅ done and no issues for  Oct 2016 - Sep 2017 , verify `unknown` counts for later dates|
| A3 | Numeric casting of `cardnumber`, `balance_before`, `value`, `balance_after` (check for letters, cast to double) | current silver, section 6 | ✅ written (2026-08-31): consolidated into `df_silver` — `cardnumber` → long (a double would lose precision on long card ids), `value`/balances → double; failed casts → NULL, counted in the consolidation checks. Pending run |
| A4 | Mapping of categoricals to canonical values:  `account_name`, `emisor`, `operator`, `card_type`, and trim of `line`, `station`, etc. | current silver, sections 7–8 | ✅ written (2026-08-31): `account_name` map added to the silver notebook → `card_profile` (full-string keys, encoding/truncated variants handled, raw kept alongside; unmapped → NULL, surfaced by the consolidation checks). Pending run |



Notes:
* A4. Categorical encoding.
    * The old code converted categorical variables (emisor, operator, account_name, line, station) to numeric ids using Spark's StringIndexer, which assigns each distinct value a number based on its frequency in the data (most frequent = 0, next = 1, …), with the value↔id correspondence saved to external CSV dictionaries (emisor_dict.csv, etc.). This had two problems: (i) the ids are not stable — re-running the pipeline on different data changes the frequencies and can silently reassign the ids, breaking any downstream code that hardcodes them (e.g. account_name_id == 3 for Anonymous); (ii) the data is unreadable without the external dictionary file.
    * In the new pipeline, silver instead maps each raw value to a canonical string via explicit dictionaries written in the code (e.g. anything containing "(3200101)" → "(3200101) Bancolombia"); unexpected new values fall to NULL and are caught by checks instead of silently receiving a new id.
    * Storage/processing cost is not a concern inside Databricks: Parquet/Delta stores low-cardinality string columns with dictionary encoding, so they are physically stored as integers anyway.
    * The only place where numeric really matters is at export (CSV has no dictionary encoding, and string variables are memory-expensive in Stata). There we will export stable numeric codes + value labels (the Stata model). . In practice this will affect few variables anyway: the exported card×month panel is mostly numeric constructed variables.
* A4. `account_name` map — bronze distinct values checked (2026-08-31):
    * The anonymous profile raw value is `"(001) Anonymous"`.
    * The code that comes in parenthesis is **ambiguous** for this variable — `(001)`, `(003)`, `(006)`, `(029)` each cover two different profiles — so the map must key on the **full string**, and the Stata export codes for this variable are assigned by us (not extracted from the parentheses).
    * Variants to handle in the map: encoding-corrupted strings (`BogotÃ¡`, `Bogot‡`, `CrÃ©dito` → same canonical as their clean versions; ~58 rows); truncated code-only values (`(005) `, `(004)`, `(101)` → the unique profile matching that code; `(000)`, `"1"`, `null` → NULL; ≤53 rows each).
    * New profile not in the old profile dictionary (old-code's `account_name_dict.csv`) `(101) Adulto PV`, 34.6M rows. 
    * Agreed two-step design: A4 stays a **1-to-1 cleaning map** (fixes encoding/truncation only, loses no detail); the **analytical grouping** of profiles happens in construction (E5 `profile_group`).

## B. Phase 1b — Cleaning 1: data quality

| # | Step | What old-code does | Impact (in old-code data) | Verdict |
|---|---|---|---|---|
| B1 | Consecutive duplicates | Flag a row as duplicate if, within the same card ordered chronologically, the previous row has the same exact timestamp AND the same `station_id` (cleaning-notebook version) or the same `line` (parquet-notebook version); keep the first occurrence, drop the rest | not reported | 🔧 **agreed definition: card + exact timestamp only**, drop all but the first. See note below. |
| B2 | Rows with missing values | old-code (create_parquet notebook, "Handle NAs" cells) decide after visual inspection of each case, seeing if they were all null rows or if they seem a corrupted file | tiny in both codes | ✅ systematic version, written into the silver notebook: compute **% of missing content columns per row**; **drop rows with 100% missing**; display the distribution and a sample of high-missing  rows to inspect and decide case by case. Pending run |


Notes:
* B1. Dedup.
    * Rationale of only dups by card and timestamp: two validations by the same card in the same second are physically one event at most, whatever the station field says; requiring station/line to match makes the dedup fragile to formatting differences across deliveries, and keeping both rows would inflate `n_trips`.
    * Diagnostics to report when running the dedup. Each dropped row is compared against **the row kept** in its (card, timestamp) group, so the diagnostics describe both sides of the pair:
        * Number of dropped duplicates **per month**, and number of **kept rows that had ≥1 duplicate** (= real events affected) per month (a spike in one month would point to a delivery problem rather than random device double-writes).
        * Among dropped duplicates: how many had **matching vs differing `station`** relative to the kept row (differing station = the case the old station-based rule would have missed).
        * Among dropped duplicates: how many came from the **same vs a different `_source_file`** than the kept row (same file = device double-write; different file = overlap between deliveries, worth investigating if the count is large).

## C. Phase 1c — Cleaning 2: analysis-oriented tags

Separate notebook, producing a window-restricted silver table. **Its first step is filtering to the analysis window** (Oct 2016 – Sep 2017 for now) — which is what C1 becomes. Everything else is implemented as **tags** (old-code dropped; we tag and decide the drop at analysis time), computed on the window so that missing/broken months outside it cannot contaminate them (window principle above). Includes both the card-level tags (C2–C4) and row-level, period-dependent ones (C5–C7).

| # | Tag | Old-code definition | Impact (old-code) | Verdict |
|---|---|---|---|---|
| C1 | Time window | Drop outside [Oct 1 2016, Sep 30 2019) | 7,067 transactions | ✅ resolved by design: the window filter IS the first step of this notebook (Oct 2016 – Sep 2017), parameterized for when the window expands. Filter using filenames as we saw there were duplicate files when using Fecha_Clearing. |
| C2 | Infrequent users | old-code: < 12 **transactions** over the whole period (25% of cards, 0.9% of transactions). `data-clean.py` (2020–2024 pipeline, our own precedent): present on < 12 **distinct days** per year (< 6 for the half-year 2024), computed within the period of interest; card kept if it meets the minimum in ANY year. Documented comparison there: days criterion drops 31% of cards (<2% of transactions) vs 22% with the transactions criterion — **days was preferred** | see left | 🔧 follow the `data-clean.py` precedent: **< 12 distinct days within the (12-month) analysis window**, as a tag |
| C3 | Superswipers | > 100 transactions in one day, OR > 20/day on more than 2 days | 2,564 cards, 3.7M transactions | ✅ Keep threshold. Tag. See note below on the `data-clean.py` variant and its bug |
| C4 | fraud_flag | More than 2 times: [transfer_time < 5.75 min more than once] and [> 9 transactions that day] (`old-code/generate_variables.py:164-169`) | not reported | ❌ Never used for filtering in clean_new_data - discard and use C3 filters instead|
| C5 | Implausible balances | old-code dropped if `balance_before` > 1,000,000 COP (~300 USD, max rechargeable); it also looked at `balance_after` > 1M but that filter did not make it into the final script | ~65k transactions | 🔧 Tag (old-code dropped) and check in which dates this happens.  |
| C6 | Impossible fares by period | old-code dropped `value`s that did not exist under the fare policy in force: 200/1450/1650/2200 before Apr 2017; 700/1000/1550/1700 between Apr 2017 and Oct 2017; 900 and 1600 always (before Oct 2017) | tiny: between 2 and 235 transactions per rule | 🔧 Tag to see how many, and drop (same as for report). Re-derive the fare×period table against the full data and unify it with the modal-fare table of `Construction_2017_subsidy_fares.do` (one canonical fare table for everything). |
| C7 | Value 0 before the policy | old-code did NOT drop: created tag `early_zero` ($0 transfers did not exist before Apr 2017; concentrated on trunk lines 2/3/5, proportional to traffic → looks like a transfer-recording error) | many, across many accounts | ✅ keep as tag. |

Notes:
* C3. `data-clean.py` (2020–2024) implemented a variant: > 100 in one day, OR > 20/day on **2+** days (old-code required **more than 2**, i.e. 3+). ⚠️ It also has a copy-paste bug: `more20swipes` is defined with `count > 100` (`data-clean.py:330-331`), so its second criterion actually flags 2+ days with >100 swipes, not >20 — the markdown there says >20 but the code does >100. The new implementation follows old-code's stated rule (>100 once, or >20 on more than 2 days), written correctly. If the 2022–2024 dataset built by `data-clean.py` is ever reused, that bug should be fixed there too.

### D. The profile-switches problem (explanation + pending decision)

Summary of what old-code found:

**The problem.** A card should have a single profile (`account_name`), or "plausible" changes (e.g. anonymous → personalized: you bought it anonymous and later registered it). But the data shows cards switching from personalized (e.g. Adulto) to **anonymous** and back — impossible in real life. Old-code's diagnosis: nearly all implausible switches are Adulto→Anonymous, they occur on **trunk transactions** (all issuers except Angelcom, all stations, no day/hour pattern). Implicit conclusion: trunk sometimes mis-record the profile as anonymous; zonal (SITP) transactions record the profile correctly.

**Old-code definitions:** `implausible_switch` = anonymous transaction whose next transaction is not anonymous (personalized→anonymous→...); `plausible_switch` = anonymous→personalized as the last change.

**The two imputations (they differ!):**

1. `old-code/clean_new_data.py:45-48` (repo version): if the card has any implausible switch, impute to the WHOLE card the profile of its **first zonal (non-trunk) transaction** (`first_non_trunk`) — because zonal records are trustworthy.
2. Final version in the cleaning notebook (cell 155, the one written out as the script at the end): `account_name_id_imputed` = anonymous if **any later transaction** is anonymous; i.e. it finds the last anonymous transaction and marks everything before it as anonymous.

These are different rules with opposite implications: (1) assumes the card is personalized and the "anonymous" records are noise; (2) assumes that if it ever shows up anonymous, it was anonymous from the start up to that point. **We must decide which one (or a third).**

**What actually ran (audit of the old pipeline):**

| Piece | Rule it contains | Did it run? |
|---|---|---|
| Cleaning notebook, interactive cells (131–140, 153) | rule (2) | yes, interactively — with diagnostics and examples |
| create_parquet notebook, `spark_handler.clean()` (cell 116) | body missing — no `clean()` method exists in the repo's `spark_df_handler.py` | **yes — this was the production run** |
| `old-code/clean_new_data.py` (repo file) | rule (1) `first_non_trunk` | no evidence it ever ran |

Even though the production `clean()` body is lost, the downstream evidence points to **rule (2)**: `account_fixed` in the sample data is described as "corrects some cards misclassified as **Adulto** that should be **Anonymous**" — exactly rule (2)'s output direction (rule (1) corrects in the opposite direction). So the old pipeline most likely ran rule (2)… **and then the sample analysis discarded its output**: `Construction_2017_basics.do:101-104` tabulates `account_fixed` vs `card_profile`, notes "don't know how this was built", drops it, and uses the original profile.

**Why it matters:** the profile defines the treatment groups (apoyo/mayor/adulto). Adulto cards will be the comparison group and we cannot risk including non-adulto cards there.

**DECIDED (2026-08-31):**
 1. **Baseline: original profile, NO imputation** — same as the sample analysis, which discarded `account_fixed` and used `card_profile` as recorded. Anonymous records are tolerated (see the exclusivity rule in E12): they don't change a card's classification, matching how the sample behaved.
 2. Tag implausible and plausible switches per card, and check how many adulto cards carry an implausible switch.
 3. **Robustness: rule (2)** ("future anonymous", the rule the old production pipeline most likely ran) — rebuild the classification with the imputed profile and compare. Additionally, excluding tagged switcher cards from the comparison group is a second robustness check.


## E. Phases 2–3 — Construction and aggregation (Spark), by output level

Implementation note: the natural notebook cut is after the transaction-level subsection (everything below it collapses rows); whether construction and aggregation are one or two notebooks is a practical choice to make when writing them.

### Level 0 — Auxiliary input: canonical fare table

| # | Variable(s) | Definition | Source | Verdict |
|---|---|---|---|---|
| E1 | Subsidized-fare table | Modal fares (top 2 = zonal/troncal) by profile {adulto, apoyo, mayor} × fare period {Oct16–Mar17, Apr17–Jan18, Feb18–Jun18, Feb19–Sep19}, computed over each card's first 30 trips of the month. Feeds E4 (subsidized trips) and C6 (impossible fares) | `Construction_2017_subsidy_fares.do:22-161` | ✅ Do this |

### Level 1 — Transaction (one new column per row)

| # | Variable(s) | Definition | Source | Verdict |
|---|---|---|---|---|
| E2 | Time variables | month/week/day/dayofweek/hour/min/sec from transaction timestamp | `generate_variables.py:102-108` | ✅ trivial |
| E3 | `transfer` and `trip` | old-code: `transfer = value < 500`; sample: `trip = value > 300` | `generate_variables.py:111-113` vs `Construction_2017_basics.do:79` | 🔧 unify: `trip = value > 300`, `transfer = !trip` |
| E4 | `apoyo_trip`, `mayor_trip` | `value` == subsidized fare (zonal or troncal) of the period and `ever_apoyo`/`ever_mayor` == 1. **Aug 2017 = missing** (glitch: all apoyo holders got the subsidy that month) | `Construction_2017_subsidy_fares.do:167-201` | ✅ port to Spark, including the Aug 2017 rule - but also tabulate number of user paying the subsidized fare each month before cosntructing this variable|
| E5 | `profile_group` | Analytical grouping of the canonical card profiles (A4): `adulto`, `anonymous`, `apoyo`, `mayor`, `empresarial` , `discapacidad`,  `adultopv`, `estudiantil`, `menor`, `frecuente`,  `other` | new | ✅ groups decided — `adultopv` and `frecuente` are separate from `adulto` and cannot be `never` controls (G11) |
| E6 | Station fix for SITP | If operator ≠ trunk, use `station_access` instead of `station` | `generate_variables.py:172-174` | 🔧 include as a placeholder, along with deeper station cleaning (old-code's station/geo dictionaries) to implement later |
| E7 | `real_balance_after`, `negative_trip`, `negative_trip_number` | `balance_before - value`; dummy < 0; streak of consecutive negatives (up to 5) | `generate_variables.py:74-99` | 🔧 Placeholder but do not add now since the final analysis does not use them (add later) |
| E8 | `transfer_time` | Minutes since the previous transaction if it's a transfer; 0 if > 95 min | `generate_variables.py:116-124` | ❌ only used by fraud_flag which was not used in later cleaning - do not construct |
| E9 | `lost_subsidy`, `lost_subsidy_year`, `october_user`, `left_in_april` | Old-code subsidy flags over windows with hardcoded fares | `generate_variables.py:142-161` | ❌ Use the approach decided for the sample analysis|

### Level 2 — Card×month (collapse to one row per card-month)

| # | Step | Definition | Source | Verdict |
|---|---|---|---|---|
| E10 | Monthly outcomes | `n_trips` (sum of `trip`), `has_trips`, `avg_daily_trips` per card×month | `Construction_2017_basics.do:76-96` | ✅ port to Spark |
| E11 | Subsidized months | `apoyo_month`/`mayor_month`: ≥50% of the month's trips at subsidized fare OR == 30 subsidized trips; apoyo/mayor exclusivity | `Construction_2017_subsidy_fares.do:204-238` | 🔧 use `>= 30` |

Note:
* It is important to use `>= 30` in E11 because before the policy change users were allowed up to 40 susbsidized trips. And the spirit of this condition is to include users who may travel a lot and thus subsidized trips is less than 50% of their monthly trips.

### Level 3 — Card (one row per card)

| # | Step | Definition | Source | Verdict |
|---|---|---|---|---|
| E12 | `ever_*` per profile group + exclusivity | Sample-code: per-card max of dummies for {adulto, apoyo, mayor} only; if a card is "ever" more than one of the three, all set to missing (excluded). Mixtures with ANY other profile (anonymous, empresarial, …) were silently ignored | `Construction_2017_basics.do:107-125` | 🔧 use the ORIGINAL profile (no imputation, per §D) and **extend the exclusivity to all non-anonymous profile groups** (E5): a card is classified only if it is "ever" exactly ONE non-anonymous group; anonymous records never break exclusivity (known trunk glitch, tolerated as in the sample). So "adulto" = always adulto except anonymous records. Tag switcher cards (§D) |
| E13 | Presence flags | `in_6m_bef` (card appears in months −6 to −1), `in_6m_aft` (months 0 to 5) relative to Apr 2017 | `Construction_2017_basics.do:60-73` | ✅ port; parameterize the reform date and windows |
| E14 | Spending by window | `tot_value_no_tr_*`: total paid on trips (no transfers) in windows −6/−1, 0/5, 6/11, 12/17 | `Construction_2017_basics.do:138-167` | ✅ port |
| E15 | Subsidized months in pre/post windows | counts `*_m_in_6m_bef`, `*_m_in_18m_aft` of subsidized months (E11) and threshold indicators | `Construction_2017_subsidy_fares.do:204-238` | 🔧 port; note the global threshold `$sub_n_trips_cond = 1` |
| E16 | Treatment groups | `kept/lost/gain/never` per type based on pre/post subsidized months (E15) + `ever_*` (E12) | `Construction_2017_balanced_panel_with_treat.do` + README §6 | ✅ port (Spark or Stata? it's lightweight post-aggregation — decide based on where the panel ends up) |
| E17 | Card-level dataset | One row per card: profile, ever_*, presence flags, cleaning-2 tags, total counts | new | ✅ for ALL cards (allows accounting of sample in/out) |

### Level 4 — Balanced card×month panel

Note: filtered to those in the analysis sample
* `in_6m_bef == 1` and `in_6m_aft == 1`
* not a superswiper (C3 tag == 0) — was a hard drop in old-code, now applied here
* not an infrequent user (C2 tag == 0, ≥12 distinct days in the window) — same
* single-profile card: `ever_*` non-missing (cards with >1 profile among {adulto, apoyo, mayor} were set to missing by the exclusivity check, E12, and cannot be assigned to any group)
* assigned to a treatment or control group: `treatment` ∈ {apoyo_kept, apoyo_lost, apoyo_gain, mayor_kept, mayor_lost, mayor_gain, never} — cards capturing none (anonymous-only, empresarial, apoyo below thresholds, etc.) drop out; in practice each regression further restricts to {one treatment group} vs never
* robustness only (not baseline): exclude cards tagged as implausible switchers (§D)

| # | Step | Definition (verdict) | Source | 
|---|---|---|---|
| E18 | Balanced panel | ✅ Build the balanced card×month grid. Month-level vars (`dist_months`, `before`/`after`, `period`) are constructed at this step from `ymonth`, deterministic functions of (month, reform date). Card-level variables are NOT included: they live in the card-level table (E17) and are merged at analysis time |replaces `Construction_2017_balanced_panel.do`, where card- and month-level vars lived in one flat Stata file | 
| E19 | Zero-coding | ✅ Synthetic rows: `n_trips=0`, `has_trips=0`, `avg_daily_trips=0`, for ALL fillin rows — including months before `first_active_month`. Keep `first_active_month` in the card-level table (E17) so the analysis can restrict to months ≥ first_active_month as a robustness check | `Construction_2017_balanced_panel.do:58-75`  |

### Level 5 — After the person-level merge (outside Databricks)

| # | Step | Definition | Source | Verdict |
|---|---|---|---|---|
| E20 | Sisben categories | `sisbenIII_range` (1–15, 16–25, 25–31, 0=missing) | `Construction_2017_basics.do:130-136` | 🔧 note: the 25/25 boundaries overlap (25 falls in category 2 by execution order) — verify intent |



### Notes:
* E9. Old vs new subsidy flags 
    * **Old-code (used in 2017 report)** built card-level flags
        * `lost_subsidy_year = 1` if an Apoyo card has NO transaction with `value` in the hardcoded range 1450–1650 (the post-reform SISBEN fares) after Apr 1 2017
        * `october_user` (any Apoyo transaction before Nov 2016) 
        * `left_in_april` (no Apoyo transaction after Apr 1 2017) were window auxiliaries of the old sample design.
    * **Sample-code** builds the same concept in three levels:
        * A fare table derived from the data — modal fares (top-2 = zonal/troncal) by profile × fare period — instead of a hardcoded range;
        * `apoyo_trip`/`mayor_trip` = transaction at exactly that period's subsidized fare, with Aug 2017 set to missing (subsidy glitch, which old-code ignored)
        * subsidized *months* (≥50% of trips or 30 subsidized trips) counted within pre/post windows with thresholds → `kept/lost/gain/never`.
    * The new approach wins on: robustness to noise (a month-level majority + a months threshold, vs a single transaction), fare validity across periods (getting fare table from data). `october_user`/`left_in_april` are replaced by the presence flags `in_6m_bef`/`in_6m_aft`.

## F. Phase 4 — Export and analysis

- Export balanced panel and card-level dataset
- Sisbén III merge at the person level: outside Databricks
- Final analysis in Stata adapting `sample-code/analysis/`.


## Output tables

| Table | Level | Content | Universe |
|---|---|---|---|
| T1. Silver 2016–2019 | transaction | types & formats (1a) + Cleaning 1 applied (1b: dedup, empty rows dropped, re-delivery files excluded) | all of 2016–2019 |
| T2. Window silver | transaction | T1 filtered to the analysis window (C1, by filenames) + Cleaning-2 tag columns (C2–C7) + Level-1 constructed columns (E2–E6) | all transactions in the window |
| T3. Monthly outcomes | card×month, observed months only | `n_trips`, `has_trips`, `avg_daily_trips` (E10); `apoyo_month`, `mayor_month` (E11) | card-months with ≥1 transaction |
| T4. Card-level dataset | card | `card_profile`/`profile_group`, `ever_*` (E12), presence flags (E13), spending windows (E14), subsidized-month counts pre/post (E15), treatment group (E16), cleaning-2 tags (C2–C7 aggregated), `first_active_month` | ALL cards in the window (allows sample in/out accounting) |
| T5. Balanced panel | card×month, balanced grid | T3 joined onto the cards×months grid, zero-coded (E19); month-level vars (`dist_months`, `before`/`after`, `period`) computed from ymonth. **No card-level columns duplicated here** — merge from T4 at analysis time | export restricted to the analysis sample (see Level 4 note) |

## Codes

Notebooks/scripts to complete or create, in order:

| # | Code | Produces | Status / to do |
|---|---|---|---|
| N1 | `data-clean-silver-from2016to2019.py` (exists) | T1 | Written: 1a (dates, casting, categorical maps incl. `card_profile`), B2 missing-rows drop, diagnostics. **To complete:** paste the Sep-30 file names into `EXCLUDED_SOURCE_FILES`; add the B1 dedup (card+timestamp) with its diagnostics; **write T1 to the catalog** (today `df_silver` is only built in memory); run end to end |
| N2 | window + Cleaning-2 notebook (new) | T2 | To create: read T1, filter to the window by filenames (C1), compute tags C2–C7, add Level-1 constructed columns (E2–E6) incl. profile imputation + switch tags (§D), save T2 |
| N3 | construction & aggregation notebook (new; may be split in two) | T3, T4, T5 | To create: fare table E1 (with the E4 pre-tabulation of users at subsidized fares per month); card-level vars E12–E16; monthly outcomes E10–E11; balanced panel E18–E19; save T3–T5 |
| N4 | export cell/notebook (new) | CSVs | To create: export T5 restricted to the analysis sample + T4 in full, with stable numeric codes + value labels for Stata (see A4 note) |
| S1 | Sisbén merge script (new, outside Databricks) | analysis dataset | To create: merge exported T4/T5 with the card→person crosswalk and Sisbén III scores; build `sisbenIII_range` (E20) |
| S2 | Stata analysis (adapt existing) | results | Adapt `sample-code/analysis/*.do` to the new panel (variable names, sample filters now explicit) |

## G. Open decisions  (checklist for discussion based on some discrepancies between different codes)

1. ✅ **RESOLVED (updated 2026-08-31, supersedes earlier rule-(1) proposal).** §D/E12: baseline = **original profile, no imputation** (as the sample analysis, which discarded `account_fixed`), with exclusivity extended to all non-anonymous profile groups (anonymous mixtures tolerated — known trunk glitch). Tag implausible/plausible switchers. Robustness: (a) rebuild classification with rule (2) imputation (the rule the old production pipeline most likely ran); (b) exclude tagged switcher cards from the comparison group.
2. ✅ **RESOLVED (2026-08-31).** C5 (ex B2): balances > 1M — tag (in the Cleaning 2 window notebook), and check in which dates it happens.
3. ✅ **RESOLVED** (C2 verdict): infrequent user = **< 12 distinct days within the 12-month analysis window** (days criterion, per the `data-clean.py` precedent). Tag.
4. ✅ **RESOLVED** (C3 verdict): keep old-code thresholds (>100 in a day, or >20/day on more than 2 days). Tag. Fix the `data-clean.py` bug if that dataset is reused.
5. ✅ **RESOLVED** (C4/E8 verdicts): discard fraud_flag (use C3 instead); do not construct transfer_time.
6. ✅ **RESOLVED** (E7 verdict): negative_trip vars not used in the final analysis — placeholder only, not built now.
7. ✅ **RESOLVED.** E11/E15: `>= 30` for E11 (see its note: pre-reform cap was 40); subsidized-months threshold `$sub_n_trips_cond = 1`, same as the sample analysis.
8. ✅ **RESOLVED (2026-08-31).** E19: zero-coding follows the sample-analysis rule as is (zeros on all synthetic rows, including before `first_active_month`); `first_active_month` kept in the card-level table for a robustness restriction at analysis time.
9. ✅ **RESOLVED (2026-08-31).** B1: duplicate definition fixed as **card + exact timestamp** (drop all but the first), robust to station/line formatting differences; with per-month, station-match and source-file diagnostics reported at dedup time (see B1 note).
10. ✅ **RESOLVED (2026-08-31).** Completeness check for Oct 2016 – Sep 2017: the 12 expected monthly files (`10_ValidacionesOct2016.csv` … `09_ValidacionesSept2017.csv`) are all present in bronze. The ~10 extra files dated Sep 30 2017 with clearing_date in the window are **confirmed duplicates**: ~100% of their rows (≥99.96% per file) match a monthly-file row on card + exact transaction timestamp. They failed the exact-content match (0%) only because the delivery uses different representation conventions — eyeball check: for zonal trips the monthly files repeat the route in `station` (e.g. `(426) Ruta 18-13 lomitas`) while the Sep-30 delivery carries a physical stop code (e.g. `(1785) 472A01_CE|472A01`) and an abbreviated line label. **Decision: exclude the extra files from the window**, implemented as a `_source_file` filter when building silver. Note for B1: a card+timestamp+station dedup would NOT catch cross-delivery duplicates precisely because `station` differs across deliveries; excluding the files at the source avoids the issue entirely.
11. ✅ **RESOLVED.** E5 `profile_group`: `(101) Adulto PV` and `(014) Usuario frecuente` are their own groups (`adultopv`, `frecuente`), NOT counted as `adulto` (so neither can be a `never` control) — regardless of when they appear in the data. Informational check still to run: transactions and distinct cards by month for these two profiles, to know their size within the window.

## Notes on old-code (so we don't lose them)

- **The reference implementation of the old pipeline is its notebooks**: `no-outputs_create_parquet_files_new_data.ipynb` (ingestion + variable generation) and `no-outputs_data_cleaning_new_data.ipynb` (cleaning) — that is where the logic actually ran. Do not trust the standalone script `clean_new_data.py`: it exists in two versions — the file in `old-code/`, and another that the cleaning notebook generates at its end (via `%%writefile`, a Jupyter command that saves a cell's code to a .py file instead of running it) — and both carry a copy-paste error (they reference `clean_df`, a variable that only existed inside the notebook session, so they crash if run on their own). The two versions also disagree on substance: the repo file has no duplicate drop and uses §D imputation (1); the generated one includes duplicates and uses imputation (2).
- The old cleaning notebook documents the impact of each filter with counts — use it as a reference for expected magnitudes when running on bronze.

