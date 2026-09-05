# Cleaning and construction decisions inventory

Goal: unify in one place all the cleaning and variable-construction decisions that appear in:
* code for 2017 report in `Colombia-BRT-IE` (currently and temporarily in `old-code/`)
* `data-clean-silver-from2016to2019.py` (current silver notebook in Databricks)
* code for 2017 analysis with a sample in Stata in `Colombia-BRT-IE` (currently and temporarily in `sample-code/)

It records what we decided to keep, change, or discard in the new pipeline over **all** the data (bronze 2016–2019). All decisions here are final (the only open items are the sizing checks under "Pending informational checks").

Icon convention in the verdict columns: ✅ kept as in the source code | 🔧 adapted/changed vs the source code | ❌ discarded.

Principles:

- **Structural row-level cleaning** (parsing, casting, duplicates, fully-empty rows, excluding source files whose content duplicates other files) runs over **all** of 2016–2019: these steps are local to each row, so incomplete months elsewhere cannot contaminate them. Since 2026-09-01, only the analysis window is **saved** to T1 (the window filter is the last step of N1); expanding the window means re-running N1 with the new file list.
- **Card-level, window-dependent constructs** (infrequent users, superswipers, card classification, profile imputation, subsidy pre/post flags) are a different story: they ARE contaminated by incomplete data (a card can look "infrequent" just because its 2018 trips are in missing/broken files). These are therefore computed **parameterized to the analysis window** (currently Oct 2016 – Sep 2017, which we verify complete), and recomputed if/when the window expands after mapping the broken/missing files.
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
| 1b. Silver: Cleaning 1 | Structural data quality: duplicates, fully-empty rows, exclusion of source files whose content duplicates other files. Drops objective errors. Runs over all of 2016–2019; the **last step filters to the analysis window** (by the 12 monthly filenames) so only the window is saved (decided 2026-09-01 — this is where C1 lives now) | Same notebook → same silver table |
| 1c. Silver: Cleaning 2 | Reads T1 (already window-restricted by N1). Analysis-oriented tags as columns: infrequent users, superswipers, implausible balances, impossible fares, early zeros. Tags only. | Other notebook → other silver table |
| 2. Construction | Transaction-level variables: imputed profile, transfer, trips, subsidized fares, subsidy flags. | Other notebook in Databricks - other table |
| 3. Aggregation | Card-level dataset (all cards) + card×month panel (balanced). | Other notebook in Databricks - other table  |
| 3b. Basket & prices | Price inputs for the elasticities, restricted to the analysis sample: transfer type per transaction, fare table with transfers, card×month trip basket, price paid per trip before the reform and price of the same basket after it (decided 2026-09-04). Reads T2, T4, T5; touches nothing upstream. | Separate notebook in Databricks (`prices-calcs.py`) - three tables |
| 4. Export & analysis | Download (or not) the restricted panel; merge Sisbén III at the person level (outside Databricks); analysis in Stata. | Outside Databricks |

## Output tables

| Table | Level | Content | Universe |
|---|---|---|---|
| T1. Silver window | transaction | types & formats (1a) + Cleaning 1 applied (1b: dedup, empty rows dropped, duplicate source files excluded) + window filter (C1, by the 12 monthly filenames, applied as the last step; cleaning and diagnostics run over all of 2016–2019 before it — decided 2026-09-01) | analysis window (Oct 2016 – Sep 2017) |
| T2. Window silver | transaction | T1 (already window-restricted) + Cleaning-2 tag columns (C2–C7) + Level-1 constructed columns (E2–E6) | all transactions in the window |
| T3. Monthly outcomes | card×month, observed months only | `n_trips`, `has_trips`, `avg_daily_trips` (E10); `apoyo_month`, `mayor_month` (E11) | card-months with ≥1 transaction |
| T4. Card-level dataset | card | `profile_groups`/`card_group` (imputed baseline, §D; E12), presence flags (E13), spending windows (E14), subsidized-month counts pre/post (E15), treatment group (E16), cleaning-2 tags (C2–C7 aggregated), `first_active_month` | ALL cards in the window (allows sample in/out accounting) |
| T5. Balanced panel | card×month, balanced grid | T3 joined onto the cards×months grid, zero-coded (E19); month-level vars (`dist_months`, `before`/`after`, `period`) computed from ymonth. **No card-level columns duplicated here** — merge from T4 at analysis time | export restricted to the analysis sample (see Level 4 note) |
| T6. Fare table | group × fare period × fare type | Modal fare, frequency and `pct_at_fare` (% of the group × period × type transactions paying exactly that fare) for `zonal`, `troncal` and the four transfer types (E22). Long format, exported as is | computed over the sample cards (T5) |
| T7. Basket | card×month, observed months only | Trips split zonal/troncal, transfers by type, amounts paid on trips, transfers and in total (E23). Intermediate, kept so other month combinations can be built without re-running | sample cards (T5) |
| T8. Prices | card | Per pre window (6m, 3m): basket sums, price paid per trip, price of the same basket at post fares; observed post price (E24) | sample cards (T5), one row each |

## Codes

Implementation context (Databricks):

- Catalog/schema: `prd_mega.scolom15`. Bronze: `bronze_validaciones_from2016to2019`. Ingestion control table: `file_classification_from2016to2019`.
- Proposed output table names (same schema): T1 `silver_validaciones_from2016to2019`, T2 `silver_validaciones_2017window`, T3 `monthly_outcomes_2017`, T4 `cards_2017`, T5 `panel_2017`, T6 `fares_2017`, T7 `basket_2017`, T8 `prices_2017`.
- Window parameters: reform month = 2017-04; analysis window = Oct 2016 – Sep 2017 (clearing dates); always parameterized, never hardcoded inline.
- Excluded source files (see B notes): the 10 files `Validacion_<operator>_<name>_20170930.csv`, matched by pattern `Validacion_\d+_.*20170930` in N1 (which prints the matched list on each run). Operators: 001 CONSORCIO EXPRESS USAQUEN, 002 MASIVO CAPITAL SUBA ORIENTAL, 004 ESTE ES MI BUS CALLE 80, 005 GMOVIL ENGATIVA, 007 ETIB, 008 SUMA, 009 TRANZIT, 011 CONSORCIO EXPRESS SAN CRISTOBAL, 012 MASIVO CAPITAL KENNEDY, 014 ESTE ES MI BUS TINTAL ZONA FRANCA.

Notebooks/scripts to complete or create, in order:

| # | Code | Produces | Status / to do |
|---|---|---|---|
| N1 | `data-clean-silver-from2016to2019.py` (exists) | T1 | Written: 1a (dates, casting, categorical maps incl. `card_profile`), B2 missing-rows drop, file exclusion by pattern (2026-08-31), B1 dedup with diagnostics, final window filter by the 12 monthly filenames (C1, 2026-09-01), write to catalog, status figures. **To do:** run end to end (the excluded-files printout should show exactly the 10 listed files; the window-filter printout exactly the 12 monthly files) |
| N2 | window + Cleaning-2 notebook (new) | T2 | To create: read T1 (already window-restricted by N1), compute tags C2–C7, add Level-1 constructed columns (E2, E3, E5, E6) incl. switch tags (§D), save T2. E4 is NOT built here: it needs the fare table (E1) and the card classification (E12), both built in N3 (decided 2026-08-31) |
| N3 | construction & aggregation notebook (new; may be split in two) | T3, T4, T5 | To create: card classification (E12); fare table E1 built inline right before its first use, E4 (`apoyo_trip`/`mayor_trip`, moved here from N2; with the pre-tabulation of users at subsidized fares per month); remaining card-level vars E13–E16; monthly outcomes E10–E11; balanced panel E18–E19; save T3–T5 |
| N3b | `prices-calcs.py` (new) | T6, T7, T8 | ✅ written (2026-09-04): sample cards from T5 with `treatment`/`card_group` from T4; T2 restricted to those cards (whole cards, so the validation sequence is complete); transfer type by lag of `is_trunk` within the transfer window (E21); fare table with transfers (E22), saved; monthly basket (E23), saved; per-card prices for the 6m and 3m pre windows plus the observed post price (E24), saved. Status figures: fare table, basket composition by month × treatment, mean prices by treatment. Checks: transfer values by type × group × period (the pricing-rule check), minutes since the previous validation by period (the time-window check), `n_trips` vs T5. Pending run |
| N4 | `data-export-2017.py` | CSVs | ✅ written (2026-09-02): panel CSV = T5 minus `avg_daily_trips`, keyed by `card_id`; cards CSV = ALL cards in the window (decided 2026-09-02: keep the full universe so who is in/out of the sample is visible in Stata), with `card_id`, `cardnumber` (string, for the Sisbén merge), `in_sample` (membership in T5, computed in Spark by join against the panel's cards — not a re-application of the filter, so it cannot drift; decided 2026-09-02), `treatment` (stable numeric codes defined explicitly in the code), `tag_infrequent`, `tag_superswiper`, `in_6m_bef`/`in_6m_aft`, `apoyo_m_in_6m_bef`/`apoyo_m_in_6m_aft`, `tot_value_no_tr_6bef`/`tot_value_no_tr_6aft`. Single CSVs on the volume (`Workspace/Construct/export_2017/`). Revised 2026-09-02: `import_2017.do` is NO longer generated here — it is a hand-maintained do-file (S1); since the treatment code map is frozen by contract, generation added Databricks round-trips without real drift protection. The Section 2 check prints the expected `label define treatment` line for comparison against S1. Added 2026-09-04: `fares_2017.csv` (T6 as is) and `prices_2017.csv` (T8 keyed by `card_id`; treatment, group and the post fares dropped — they merge from the cards file and the fares file); checks that the prices file covers exactly the panel's cards. Pending run |
| S1 | `import_2017.do` (hand-maintained, `$Cleaning_TuLlave_2017`) | `.dta` in `$PII_2017` | Drafted 2026-09-02: CSVs → `.dta` + treatment value labels (must match N4's `TREATMENT_CODES`; double-guarded). Details: `local-code/Cleaning/TuLlave_2017/README.md`. Added 2026-09-04: `prices_2017.csv` → `$PII_2017\prices_2017.dta` (keyed by `card_id`) and `fares_2017.csv` → `$Final_2017\fares_2017.dta` (no card key, straight to the PII-free folder); S3 re-keys the prices file like the panel (`card_id` → `card_nid`) into `$Final_2017\prices_2017_final.dta` |
| S2 | Sisbén merge (`$Construction_2017`) | `cards_2017_sisben.dta` + PII link file | Name-based card→person chain; the Sisbén ID only separates homonyms (one history per name × ID); Sisbén category (kept/lost/new), post-change score (from April 2017 on) and `sisbenIII_range` (E20) computed from the person-month Sisbén file; nobody excluded, every data issue is a flag; matched = name match exact or fuzzy ≥ 0.85 + the people behind the name agree on category and range (a name where one lost and another kept or is new is not matched); threshold and range cuts are parameters at the top of the do-file, cuts set on the printed distribution. OPTIONAL — S3/S4 run without it. Details: `local-code/Construction/Reform_2017/README.md` |
| S3 | Anonymize (`$Repo\Anonymize`) | `$Final_2017` datasets + PII crosswalk | Drafted 2026-09-02: salted SHA-256 of `cardnumber`; Final data keyed by `card_nid` (hash-ordered numeric id), `card_id` dropped; salt and crosswalk only in `$PII_2017`. Added 2026-09-04: the prices file is re-keyed the same way (`prices_2017_final.dta`). Details: `local-code/Anonymize/README.md` |
| S4 | Stata analysis (`$Analysis_2017`, adapting `sample-code/analysis/*.do`) | results | Drafted 2026-09-02: event studies, DiD ATEs and elasticities; Sisbén heterogeneity skips until S2 runs. Since 2026-09-04 the months dropped from the regressions (event studies, DiD, elasticities, Sisbén heterogeneity) are a single switch in MAIN (`$excl_months`, next to `$excl_infrequent`): default Mar 2017 only (base = Feb 2017); `"-1 -2"` reproduces the earlier Feb–Mar exclusion, `""` keeps all months; outputs carry a suffix (`exMar`, `exFebMar`, `all`) so runs with different exclusions coexist. Trend-adjusted event studies (`xtevent`, linear differential pre-trend, added 2026-09-03 from the sample-code detrend files) parked 2026-09-04: removed from MAIN's call list, file kept with its own hardcoded variants; not pursued for now. Pending from the sample code: HonestDiD smoothness bounds. Rewritten 2026-09-04: `Analysis_Reform_2017_elasticities.do` runs on the fares and prices files (no hand-typed fare globals) — price table by group (apoyo_kept in total and by Sisbén range, apoyo_lost, never) × window (6m, 3m) × concept (the fares zonal/troncal/transfers, effective = paid per trip vs same basket at post fares, observed post as diagnostic), each change also net of the never-treated change (price DiD); elasticities on the effective price only, as % changes over the pre-reform values (decided 2026-09-04: no arc/midpoint version, and the basket-weighted nominal price is out for now), with the differential (net of never) price change, for the price window matching the pre months of the regressions (3m when Oct–Dec are excluded via `$excl_months`, 6m otherwise); by Sisbén range the betas come from the Sisbén DiD, now also saved as `.ster`. Needs Stata 16+ (frames). Details: `local-code/Analysis/Reform_2017/README.md` |

### Status figures

Each notebook ends with a "Status figures" section that generates these (so they regenerate on every run and always reflect the current data):

**N1 — message: "the raw data is clean and we know what we have"**
1. Daily transactions line plot, bronze vs T1 (before/after cleaning; the daily plot already in the notebook, duplicated for T1): shows the effect of the excluded files + dedup, and the data gaps in 2018–2019 that justify the Oct16–Sep17 window.
2. Month×day coverage heatmap (already in the notebook): the map of which days have data, with the verified window marked.
3. Cleaning waterfall: bar per stage — rows in bronze → after excluding duplicate source files → after dropping fully-empty rows → after dedup (= T1) — with row counts and % dropped at each step.
4. Dropped duplicates per month (from the B1 diagnostics): flat = sporadic device double-writes; a spike in one month = a file loaded twice, investigate.

**N2 — message: "this is what the analysis sample looks like"**
1. Incidence of each tag: % of cards and % of transactions flagged by infrequent (C2), superswiper (C3), balance > 1M (C5), impossible fare (C6), early zero (C7). The key figure of this stage: how much each cleaning decision weighs.
2. Transactions by `profile_group` per month (lines or stacked area) — this doubles as the pending informational check on `frecuente`.
3. `balance_before` > 1M rows by date — this is the pending informational check of C5.
4. Distribution of distinct days per card in the window, with a line at 12: shows what the infrequent-user threshold cuts.

**N3 — message: "the treatment groups exist and make sense"**
1. The fare table, visual: modal fares by profile × fare period with their frequencies (E1) — validates the identification of subsidized trips.
2. % of apoyo cards' trips at the subsidized fare, by month: the April 2017 reform and the August 2017 glitch must be visible here — best single sanity check of the whole pipeline.
3. Sample funnel: all cards → in window → `in_6m_bef & in_6m_aft` → not a superswiper → assigned to a treatment group. Answers "how many are in/out of the sample".
4. Number of cards per treatment group (kept/lost/gain/never, for apoyo and mayor).
5. Mean `n_trips` per month by treatment group: raw pre-trends — the preview of the final analysis.

**N3b — message: "we know what each group paid and what the reform did to their price"**
1. The fare table with transfers: trip fares and the four transfer fares by group × period (E22) — the reform's fare change, read from the data.
2. Basket composition by month × treatment group: share of zonal trips, transfers per trip, zonal → troncal transfers per trip, amount paid per trip. Transfers per trip must jump in April 2017 (they became free) and the amount paid must move in opposite directions for kept and lost.
3. Mean price per trip by treatment group: paid before (6m, 3m), same basket at post fares, observed after — the preview of the price table used by the elasticities.

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
* Excluded duplicate source files (window completeness check, resolved 2026-08-31). The Oct 2016 – Sep 2017 window is fully covered by 12 monthly files (`10_ValidacionesOct2016.csv` … `09_ValidacionesSept2017.csv`), all present in bronze with no zero-row days. ~10 extra files dated Sep 30 2017 also carry clearing_dates in the window but are **confirmed duplicates**: ~100% of their rows (≥99.96% per file) match a monthly-file row on card + exact transaction timestamp. They fail an exact-content match (0%) only because those files use different formatting conventions — e.g. for zonal trips the monthly files repeat the route in `station` (`(426) Ruta 18-13 lomitas`) while the Sep-30 files carry a physical stop code (`(1785) 472A01_CE|472A01`). **Decision: exclude those files entirely** via the `_source_file` filter in the silver notebook (`EXCLUDED_SOURCE_FILES`). Note: a card+timestamp+station dedup would NOT catch these duplicates (station is formatted differently in each file); excluding the files avoids the issue.
* B1. Dedup.
    * Rationale of only dups by card and timestamp: two validations by the same card in the same second are physically one event at most, whatever the station field says; requiring station/line to match makes the dedup fragile to formatting differences across source files, and keeping both rows would inflate `n_trips`.
    * Diagnostics to report when running the dedup. Each dropped row is compared against **the row kept** in its (card, timestamp) group, so the diagnostics describe both sides of the pair:
        * Number of dropped duplicates **per month**, and number of **kept rows that had ≥1 duplicate** (= real events affected) per month (a spike in one month would suggest a whole file was loaded twice or two files overlap, rather than random device double-writes).
        * Among dropped duplicates: how many had **matching vs differing `station`** relative to the kept row (differing station = the case the old station-based rule would have missed).
        * Among dropped duplicates: how many came from the **same vs a different `_source_file`** than the kept row (same file = device double-write; different file = the same trip appears in two source files, worth investigating if the count is large).

## C. Phase 1c — Cleaning 2: analysis-oriented tags

Separate notebook, producing a window-restricted silver table. **Its first step is filtering to the analysis window** (Oct 2016 – Sep 2017 for now) — which is what C1 becomes. Everything else is implemented as **tags** (old-code dropped; we tag and decide the drop at analysis time), computed on the window so that missing/broken months outside it cannot contaminate them (window principle above). Includes both the card-level tags (C2–C4) and row-level, period-dependent ones (C5–C7).

| # | Tag | Old-code definition | Impact (old-code) | Verdict |
|---|---|---|---|---|
| C1 | Time window | Drop outside [Oct 1 2016, Sep 30 2019) | 7,067 transactions | ✅ resolved by design, moved to N1 (2026-09-01): the window filter is the LAST step of N1 — cleaning and diagnostics run over all of 2016–2019, only the window is saved to T1. Filter using filenames (the 12 monthly files), not Fecha_Clearing (duplicate-files issue). Parameterized for when the window expands (re-run N1 with the new file list). Bonus: the 2019 dual-validation files with hashed (hex) cardnumbers drop out automatically — no ad-hoc exclusion needed; the `try_cast` numeric casts stay as the safety net |
| C2 | Infrequent users | old-code: < 12 **transactions** over the whole period (25% of cards, 0.9% of transactions). `data-clean.py` (2020–2024 pipeline, our own precedent): present on < 12 **distinct days** per year (< 6 for the half-year 2024), computed within the period of interest; card kept if it meets the minimum in ANY year. Documented comparison there: days criterion drops 31% of cards (<2% of transactions) vs 22% with the transactions criterion — **days was preferred** | see left | 🔧 follow the `data-clean.py` precedent: **< 12 distinct days within the (12-month) analysis window**, as a tag. The tag does NOT filter the panel — infrequent cards (frequent among always-adulto comparison cards) stay in the sample, and excluding them is a robustness check at analysis time (see Level 4 note) |
| C3 | Superswipers | > 100 transactions in one day, OR > 20/day on more than 2 days | 2,564 cards, 3.7M transactions | ✅ Keep threshold. Tag. See note below on the `data-clean.py` variant and its bug |
| C4 | fraud_flag | More than 2 times: [transfer_time < 5.75 min more than once] and [> 9 transactions that day] (`old-code/generate_variables.py:164-169`) | not reported | ❌ Never used for filtering in clean_new_data - discard and use C3 filters instead|
| C5 | Implausible balances | old-code dropped if `balance_before` > 1,000,000 COP (~300 USD, max rechargeable); it also looked at `balance_after` > 1M but that filter did not make it into the final script | ~65k transactions | 🔧 Tag (old-code dropped) and check in which dates this happens.  |
| C6 | Impossible fares by period | old-code dropped `value`s that did not exist under the fare policy in force: 200/1450/1650/2200 before Apr 2017; 700/1000/1550/1700 between Apr 2017 and Oct 2017; 900 and 1600 always (before Oct 2017) | tiny: between 2 and 235 transactions per rule | 🔧 Tag to see how many, and drop (same as for report). Re-derive the fare×period table against the full data and unify it with the modal-fare table of `Construction_2017_subsidy_fares.do` (one canonical fare table for everything). |
| C7 | Value 0 before the policy | old-code did NOT drop: created tag `early_zero` ($0 transfers did not exist before Apr 2017; concentrated on trunk lines 2/3/5, proportional to traffic → looks like a transfer-recording error) | many, across many accounts | ✅ keep as tag. |

Notes:
* C3. `data-clean.py` (2020–2024) implemented a variant: > 100 in one day, OR > 20/day on **2+** days (old-code required **more than 2**, i.e. 3+). ⚠️ It also has a copy-paste bug: `more20swipes` is defined with `count > 100` (`data-clean.py:330-331`), so its second criterion actually flags 2+ days with >100 swipes, not >20 — the markdown there says >20 but the code does >100. The new implementation follows old-code's stated rule (>100 once, or >20 on more than 2 days), written correctly. If the 2022–2024 dataset built by `data-clean.py` is ever reused, that bug should be fixed there too.

### D. The profile-switches problem (explanation + decision)

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

**DECIDED (2026-08-31, revised 2026-09-01):**
 1. **Baseline: imputed profile, rule (2)** ("future anonymous") — a transaction counts as anonymous if the card has ANY anonymous transaction from that moment on; card classification (E12) runs on this imputed profile. Revised 2026-09-01: this replaces the 2026-08-31 baseline (original profile, no imputation) — rule (2) is the rule the old production pipeline most likely ran and correctly represents cards that traveled anonymous before registering.
 2. Tag implausible and plausible switches per card, and check how many adulto cards carry an implausible switch.
 3. **Comparison group: always adulto** — a card qualifies for the comparison group only if it is adulto in EVERY transaction under the imputed profile (no anonymous records at all), stricter than an anonymous-tolerant adulto classification. The treatment groups (apoyo/mayor) keep the anonymous tolerance of E12.
 4. **Robustness:** rebuild the classification with the original profile as recorded (the sample-analysis behavior, the pre-revision baseline) and compare. Additionally, excluding tagged switcher cards is a second robustness check — for the treatment groups only, since always-adulto comparison cards have no anonymous records and therefore no switches by construction.


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
| E4 | `apoyo_trip`, `mayor_trip` | `value` == subsidized fare (zonal or troncal) of the period and `card_group` == apoyo/mayor. **Aug 2017 = missing** (glitch: all apoyo holders got the subsidy that month) | `Construction_2017_subsidy_fares.do:167-201` | ✅ port to Spark, including the Aug 2017 rule - but also tabulate number of user paying the subsidized fare each month before cosntructing this variable|
| E5 | `profile_group` | Analytical grouping of the canonical card profiles (A4): `adulto`, `anonymous`, `apoyo`, `mayor`, `empresarial` , `discapacidad`, `estudiantil`, `menor`, `frecuente`,  `other` | new | ✅ groups decided — `(101) Adulto PV` (PV = personalización virtual, an adulto card) maps to `adulto`; the profile does not appear in the 2017 window anyway. `frecuente` is separate from `adulto` and cannot be a `never` control, regardless of when it appears in the data. Informational check to run: transactions and distinct cards by month for `frecuente`, to know its size within the window |
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
| E12 | `profile_groups` + `card_group` (card classification) | Sample-code: per-card max of dummies for {adulto, apoyo, mayor} only; if a card is "ever" more than one of the three, all set to missing (excluded). Mixtures with ANY other profile (anonymous, empresarial, …) were silently ignored | `Construction_2017_basics.do:107-125` | 🔧 On the IMPUTED profile (rule (2), §D), build `profile_groups` = the card's full history — every profile group it shows across the window, with anonymous first (e.g. `anonymous+apoyo`; chronological under the imputation, where the anonymous block always precedes the personalized tail). `card_group` assigns the analysis group by an explicit whitelist on that history: `always_adulto` ← {adulto} (adulto in EVERY transaction, no anonymous — the name is strict on purpose, required for the comparison group E16); `apoyo` ← {apoyo, anonymous+apoyo}; `mayor` ← {mayor, anonymous+mayor} (anonymous never disqualifies treatment cards — these are the only apoyo/mayor histories possible, since the imputed history has at most one transition). Any other history (mixed non-anonymous groups, anonymous-only, other profiles) gets no group. No separate `ever_*`/`always_adulto` flags: they are all derivable from `card_group`. Tag switcher cards (§D) |
| E13 | Presence flags | `in_6m_bef` (card appears in months −6 to −1), `in_6m_aft` (months 0 to 5) relative to Apr 2017 | `Construction_2017_basics.do:60-73` | ✅ port; parameterize the reform date and windows |
| E14 | Spending by window | `tot_value_no_tr_*`: total paid on trips (no transfers) in windows −6/−1, 0/5, 6/11, 12/17 | `Construction_2017_basics.do:138-167` | ✅ port |
| E15 | Subsidized months in pre/post windows | counts `*_m_in_6m_bef`, `*_m_in_6m_aft` of subsidized months (E11) and threshold indicators. Post window = 6 months (`SUB_POST_RANGE = (0, 5)`, same as the presence window; changed 2026-09-02 from the earlier 18-month idea, since the analysis window ends Sep 2017) | `Construction_2017_subsidy_fares.do:204-238` | ✅ port; threshold decided: `$sub_n_trips_cond = 1` subsidized month, same as the sample analysis |
| E16 | Treatment groups | `kept/lost/gain/never` per type based on pre/post subsidized months (E15) + `card_group` (E12); `never` (the comparison group) requires `card_group == "always_adulto"` (adulto in every transaction, never anonymous, §D) | `Construction_2017_balanced_panel_with_treat.do` + README §6 | ✅ port (Spark or Stata? it's lightweight post-aggregation — decide based on where the panel ends up) |
| E17 | Card-level dataset | One row per card: `profile_groups`/`card_group`, presence flags, cleaning-2 tags, total counts | new | ✅ for ALL cards (allows accounting of sample in/out) |

### Level 3b — Basket and prices (N3b, decided 2026-09-04)

Price inputs for the fare elasticities. Everything here is restricted to the analysis sample (the cards in T5: `never`, `apoyo_kept`, `apoyo_lost`, present before and after, not superswipers) and lives in a separate notebook that reads T2, T4 and T5, so nothing upstream is re-run. The zonal/troncal modal fares are therefore computed twice — inline in N3 over all classified cards (for `apoyo_trip`, E4) and here over the sample cards — with the same rule and parameters; the table exported to Stata is this one (the fare table *of the sample*).

| # | Variable(s) | Definition | Verdict |
|---|---|---|---|
| E21 | `transfer_type` (transaction) | For rows with `transfer == 1`: origin leg = `is_trunk` of the card's previous validation of any kind (the chain is trip → transfer → transfer), destination leg = the transfer's own `is_trunk`; `zz`, `zt`, `tz`, `tt`. `unknown` if there is no previous validation in the window, the gap exceeds the transfer time window, or an operator type is missing. Time window: **95 minutes** (the rule announced with the April 2017 reform, see note; the old code's `transfer_time` used the same cut). Not saved to T2: computed in N3b and consumed there | ✅ written |
| E22 | Fare table with transfers (T6) | Trips: top-2 modal values by `card_group` × fare period over each card's first 30 trips of the month (as E1) → `zonal` (lower), `troncal` (higher). Transfers: modal value by `card_group` × fare period × transfer type → `tr_zz`, `tr_zt`, `tr_tz`, `tr_tt`. Long format with frequency and `pct_at_fare` (% of the group × period × type transactions at that fare). A transfer type with no transfers in a group-period gets the base transfer fare (`tr_zz`) and is reported. The full value distribution of transfers by type is displayed as the check of the pricing rule: 300 for every transfer before the reform; after it, 200 for zonal → troncal and 0 otherwise (to be read off the run, not assumed) | ✅ written |
| E23 | Monthly basket (T7) | card × month with ≥1 transaction: `n_trips`, `n_zonal`, `n_troncal`, `n_transfers`, `n_tr_{zz,zt,tz,tt,unknown}`, `tot_value_trips`, `tot_value_transfers`, `tot_value_all`. **What the card actually paid**: tagged rows (early zeros, impossible fares) enter as recorded, no special handling (decided 2026-09-04) — same treatment as `n_trips` in T3, which does not exclude them either | ✅ written |
| E24 | Prices per card (T8) | For each pre window `w` ∈ {6m = months −6..−1, 3m = months −3..−1}: basket sums over the window, `p_pre_w` = `tot_value_all` / `n_trips` (transfers included in the numerator, divided by trips), `p_post_cf_w` = the same basket valued at the post-reform fares of the group the card pays after the reform (apoyo fares for `apoyo_kept`; adulto fares for `apoyo_lost` and `never`), divided by the same `n_trips`; `unknown` transfers valued at `tr_zz`. Missing when the card has no trips in the window (possible in 3m). `p_post_obs` = amount paid per trip in months 0–5 **excluding Aug 2017** (glitch): a diagnostic, its gap with `p_post_cf` is the change in the basket itself, not to be used as the price in the elasticity | ✅ written |

Notes:
* E21. Transfer time window. A Pulzo note of 5 April 2017 on the reform states that transfers became free only for personalized TuLlave cards, from SITP to SITP or from an articulated bus to SITP, "en un tiempo máximo de 95 minutos", with 200 extra when moving from a zonal bus to an articulated one; before the reform every transfer paid 300 (El Tiempo, Feb 2016). Later windows (110 min in 2022, 125 in 2024) do not apply. No source found for the pre-reform window: the notebook's histogram of minutes since the previous validation, by period, checks it against the data — mass beyond 95 minutes in the pre period means the pre window was different and `TRANSFER_WINDOW_MIN` should become per-period. The three sample groups hold personalized cards, so the personalized-only rule does not bite.
* E24. What Stata does with T6 and T8 (S4, written 2026-09-04): the price table by group and window — the fares (zonal, troncal, transfers) and the effective prices (`p_pre`, `p_post_cf`, `p_post_obs`) — for `apoyo_kept` (in total and by `sisbenIII_range` once S2 runs), `apoyo_lost` and `never`; the **price DiD** = change of the treated group minus change of `never` (never also faces a fare change: the full fares rose in April 2017), in % and in levels; and the elasticities on the **effective** price (observed pre vs counterfactual post) = % change in trips over % change in price, both with respect to the pre-reform value (decided 2026-09-04: no arc/midpoint version; the sample code never implemented elasticities, its "Elasticities" section is an empty header), the differential change as baseline. The basket-weighted nominal price was dropped from the Stata side (decided 2026-09-04). Both windows are exported; the elasticities use the one matching the pre months of the regressions (`$incl_months`): 3m when Oct–Dec 2016 are excluded, 6m otherwise — the price must come from the same window as the impacts.

### Level 4 — Balanced card×month panel

Note: filtered to those in the analysis sample
* `in_6m_bef == 1` and `in_6m_aft == 1`
* not a superswiper (C3 tag == 0) — was a hard drop in old-code, now applied here
* infrequent users (C2 tag, <12 distinct days in the window) are NOT filtered out: they stay in the panel. Many always-adulto comparison cards are infrequent, and dropping them would shrink and select the comparison group. Excluding them from the three analysis groups is a robustness check at analysis time (merge `tag_infrequent` from T4)
* classified card: `card_group` non-null (E12) — histories mixing non-anonymous groups, anonymous-only cards, and other profiles get no group and cannot be assigned to a treatment
* assigned to a treatment or control group: `treatment` ∈ {apoyo_kept, apoyo_lost, apoyo_gain, mayor_kept, mayor_lost, mayor_gain, never} — cards capturing none (anonymous-only, empresarial, apoyo below thresholds, etc.) drop out; in practice each regression further restricts to {one treatment group} vs never
* robustness only (not baseline): exclude cards tagged as implausible switchers (§D); exclude infrequent users (C2)

| # | Step | Definition (verdict) | Source | 
|---|---|---|---|
| E18 | Balanced panel | ✅ Build the balanced card×month grid. Month-level vars (`dist_months`, `before`/`after`, `period`) are constructed at this step from `ymonth`, deterministic functions of (month, reform date). Card-level variables are NOT included: they live in the card-level table (E17) and are merged at analysis time |replaces `Construction_2017_balanced_panel.do`, where card- and month-level vars lived in one flat Stata file | 
| E19 | Zero-coding | ✅ Synthetic rows: `n_trips=0`, `has_trips=0`, `avg_daily_trips=0`, for ALL fillin rows — including months before `first_active_month`. Keep `first_active_month` in the card-level table (E17) so the analysis can restrict to months ≥ first_active_month as a robustness check | `Construction_2017_balanced_panel.do:58-75`  |

### Level 5 — After the person-level merge (outside Databricks)

| # | Step | Definition | Source | Verdict |
|---|---|---|---|---|
| E20 | Sisben categories | `sisbenIII_range` (1–15, 16–25, 25–31, 0=missing) | `Construction_2017_basics.do:130-136` | Now `sisbenIII_range` = 1 up to `$sis_cut1`, 2 to `$sis_cut2`, 3 above (≤ 30.56), missing if no score; cuts are parameters of the Sisbén merge, set on the printed score distribution (defaults 15, 25, no overlap) |



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

## F. Phase 4 — Export and analysis (the local pipeline): overview

Everything after N4 runs locally in Stata, using the globals from `Colombia-BRT-IE\Subsidy Paper\Globals\directories.do`. The do-file drafts live in this repo's git-ignored `local-code/` folder; its top-level README maps each file to its Colombia-BRT-IE destination, and **each subfolder has its own README with the full detail** (inputs/outputs, decisions, specifications).

Folder convention (2026-09-02): everything from the full-data pipeline — data, outputs, and the Colombia-BRT-IE code folders — gets a `_Fulldata` suffix so it never mixes with the old 1%-sample files; the do-files redefine the affected globals right after loading `directories.do` (candidate to move there). In short: `$PII_2017` = `$Int_2017\PII` holds everything with `cardnumber`/names/documents (never shared or committed), `$Final_2017` holds the PII-free analysis datasets (keyed by `card_nid`) and is the only folder the analysis reads, and outputs go to `$Fig_2017`/`$Tab_2017`.

Run order (the four downloaded CSVs — cards, panel, prices, fares — → `$PII_2017`): S1 import → S2 Sisbén merge (optional, only for Sisbén heterogeneity) → S3 anonymize (builds `$Final_2017`) → S4 analysis. The fare-based kept/lost analysis needs no Sisbén data: the first pass is S1 → S3 → S4, and when S2 runs later, re-run S3 and S4.

## Pending informational checks (decisions already made; these just size them)

- C5: in which dates do the `balance_before` > 1M rows concentrate.
- E5: transactions and distinct cards by month for `(101) Adulto PV` and `(014) Usuario frecuente`.

## Notes on old-code (so we don't lose them)

- **The reference implementation of the old pipeline is its notebooks**: `no-outputs_create_parquet_files_new_data.ipynb` (ingestion + variable generation) and `no-outputs_data_cleaning_new_data.ipynb` (cleaning) — that is where the logic actually ran. Do not trust the standalone script `clean_new_data.py`: it exists in two versions — the file in `old-code/`, and another that the cleaning notebook generates at its end (via `%%writefile`, a Jupyter command that saves a cell's code to a .py file instead of running it) — and both carry a copy-paste error (they reference `clean_df`, a variable that only existed inside the notebook session, so they crash if run on their own). The two versions also disagree on substance: the repo file has no duplicate drop and uses §D imputation (1); the generated one includes duplicates and uses imputation (2).
- The old cleaning notebook documents the impact of each filter with counts — use it as a reference for expected magnitudes when running on bronze.

