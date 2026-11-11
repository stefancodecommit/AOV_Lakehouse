# Demo script

The live demonstration for the January presentation (slides: `docs/presentatie.pptx`,
the *Demo* slide). About **5 minutes**, in five steps. The steps and commands are in
English like the rest of the repository; the talking points (*Zeggen*) are in Dutch, the
language of the presentation.

## Before the session

**The day before**

1. `git pull` and `.venv/bin/pytest`: everything passes.
2. Make sure the data is current and clean: `.venv/bin/python -m tools.inspect_lakehouse`
   ends with `problems: none`. If not, run the pipeline (INSTRUCTIONS.md, section 2).
3. **Make the fallback screenshots** (see the bottom of this page) and put them in a folder
   on the laptop, outside the repository.
4. Export the report (`.venv/bin/python -m tools.export_report`) and keep the PDF at hand.
5. **Let Dagster catch up, with internet:** start `.venv/bin/python -m tools.demo` and
   leave it running until Dagster's *Runs* page shows nothing running. Schedule moments
   missed since Dagster last ran (the CBS fetch on the 1st, maintenance on the 15th) are
   made up as soon as it starts (INSTRUCTIONS.md, section 5). Done now, they don't start
   during the presentation: the CBS fetch needs internet, maintenance makes the lakehouse
   busy, and new CBS data would change the figures mid-demo. Then Ctrl+C.

**10 minutes before**, one command from the project folder:

```bash
.venv/bin/python -m tools.demo
```

It starts the dashboard (8501), the lakehouse viewer (8502) and Dagster (3000), waits
until all three answer (Dagster takes longest, about half a minute), and opens the
**start page** (`build/demo_start.html`): a numbered card per step below, with buttons
that each open the right place in a new tab, and a dot per service (green: it answers).
Present from that page; it stays open. Then:

- click **KVK in bronze** (step 5) once, so its data-quality statistics (~13 s) are
  cached before the audience sees them;
- check Dagster's **Runs** page: nothing running, nothing failed. A run that is busy is a
  catch-up (see the day before): let it finish;
- leave the terminal open: Ctrl+C there stops everything after the session.

Without the start page, the same places by hand: the dashboard's *Risk index* page;
`docs/lineage_explorer.html#gold:gold_risk_index.relative_risk` in a browser; Dagster's
menu **Lineage** (the asset graph); the viewer, layer bronze, table
`kvk/basis_bedrijfsgegevens`, view *Data quality*.

Browser zoom about 125%, other applications closed, notifications off. Everything runs
locally: no internet needed during the demo.

## The five steps

### 1. The risk index (1½ min) · button *Risk index*

**Show:** the dashboard page *Risk index*, sector level *Sections*.
- Point at the counts: 5 of 19 sections above national; 1 rising.
- The chart: red above national in every quarter, blue below, the band per sector.
- Switch on **Final figures only** in the sidebar: the rising sector changes from
  *S Overige dienstverlening* to *B Delfstoffenwinning*. Switch it off again.

**Zeggen:** "Dit is wat de data-analist ziet: per sector het relatieve risico tegenover
landelijk, met de bandbreedte over acht kwartalen. Rood ligt in elk kwartaal boven
landelijk. Kijk wat er gebeurt als ik alleen definitieve cijfers toon: een andere sector
stijgt. En een terugtoets op de historie laat zien dat het oordeel hoger of lager een jaar
later standhoudt, maar dat een trend niet voorspelt. Een trend is dus een reden om te
kijken, geen voorspelling."

### 2. One sector explained (1 min) · button *Sector Q*

**Show:** under *One group in detail*, choose **Q Gezondheids- en welzijnszorg**.
- Read the explanation sentence aloud (40% more absence, stable, 88% run by natural
  persons).
- The chart below: the quarterly relative risk around 1.4, with the national line at 1.00.
- Optional, if time: page *Behind the numbers*, sector Q: what differs beyond the margin
  of error.

**Zeggen:** "Elk getal wordt in één zin uitgelegd, zodat de analist het met vertrouwen aan
de manager kan doorgeven. De lijn laat zien dat de zorg al twee jaar ongeveer 40% boven
landelijk ligt."

### 3. Where a number comes from (1 min) · button *Lineage explorer*

**Show:** the lineage explorer tab, opened on `gold_risk_index.relative_risk`.
- Follow the chain to the left: gold `relative_risk` ← silver `fct_absence_rate.absence_pct`
  ← bronze `80072ned_typeddataset.Ziekteverzuimpercentage_1`.
- Click the bronze field: everything that depends on it appears on the right.

**Zeggen:** "Voor elke kolom is vastgelegd waar hij vandaan komt. Dit relatieve risico komt
uit één CBS-veld; omgekeerd zie ik meteen wat er allemaal verandert als het CBS dat veld
aanpast. Een test faalt zodra een nieuwe kolom geen herkomst heeft."

### 4. The workflow (1 min) · buttons *Asset graph*, *Checks silver_cbs*, *Schedules*

**Show:** Dagster, the asset lineage graph (Dagster's menu: **Lineage**).
- The flow: `cbs_bronze` and `sbi_crosswalk` → `silver_cbs` → `gold`; `kvk_bronze` loaded by
  a person.
- `silver_cbs`, tab **Checks**: the checks (unique keys, keys in dimensions, one run).
- **Automation** (Dagster's menu): the monthly schedule; silver and gold only after new data and passed checks.
- Optional (20 s): **Runs** (Dagster's menu): a scheduled run made up later, e.g.
  `cbs_monthly` for 1 November, started on 9 November at 19:43.

**Zeggen:** "Dagster haalt maandelijks nieuwe cijfers op. Is er niets nieuws, dan gebeurt
er niets. Een laag wordt pas gebouwd als de controles van de laag eronder zijn geslaagd, zo
bereiken foute gegevens het dashboard niet."

*Optioneel:* "En staat mijn laptop op de eerste uit? Dan haalt Dagster het in zodra hij weer
draait: deze run was gepland op 1 november en is op 9 november gestart."

### 5. Data quality (½ min) · buttons *KVK in bronze*, *Silver kvk_company*

**Show:** the lakehouse viewer: layer *bronze*, table `kvk/basis_bedrijfsgegevens`, view
**Data quality** (takes ~13 s the first time; open it once before the session so it is
cached).
- *Duplicate rows*: 381,513 identical rows in the KVK file.
- Switch to layer *silver*: *padded text* is 0 everywhere; in bronze it isn't.

**Zeggen:** "Bronze bewaart alles zoals ontvangen, ook de opvulspaties van het CBS; silver
schoont op. In het KVK-bestand zitten 381.513 identieke rijen. Zonder KVK-nummer weet ik
niet of dat dubbelingen zijn, dus ik heb ze gemarkeerd en niet verwijderd."

## If something fails live

| Problem | Fallback |
|---|---|
| A page doesn't load or is slow | switch to the screenshot of that step; continue talking |
| Dagster doesn't start | skip step 4, or show its screenshot; the workflow is also on slide 4 |
| A run starts or fails in Dagster during the demo | a missed schedule moment being made up (the day before was skipped); leave it, say that this is the catch-up, continue with step 5; a failed CBS fetch only means no internet |
| A dot on the start page stays red | that service didn't start: its log is in `build/demo_logs/`; the other steps still work |
| The start page doesn't open, or shows HTML code | it was opened in an editor: open it in a browser, `xdg-open build/demo_start.html` (macOS: `open`) |
| The viewer's statistics take too long | show the screenshot; mention the 381,513 rows |
| The laptop or projector fails | present from the PDF of the slides and the report |

**Screenshots to make the day before** (one per step): the risk index page (with *Final
figures only* off and on), sector Q in detail, the lineage explorer on
`gold_risk_index.relative_risk`, the Dagster asset graph, and the viewer's data-quality
view of the KVK table.

## After the session

Ctrl+C in the terminal running `tools.demo`: it stops the dashboard, the viewer and
Dagster.
