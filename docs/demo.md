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

**10 minutes before**, in three terminals from the project folder:

```bash
.venv/bin/streamlit run dashboard/app.py                    # dashboard  http://localhost:8501
.venv/bin/python -m tools.lakehouse_viewer                   # viewer     http://localhost:8502
DAGSTER_HOME=$PWD/orchestration .venv/bin/dagster dev -f orchestration/definitions.py
                                                             # Dagster    http://localhost:3000
```

Open in the browser, each in its own tab, in this order:

1. <http://localhost:8501> (dashboard), go to **Risk index**
2. `docs/lineage_explorer.html#gold:gold_risk_index.relative_risk` (open the file, then
   add the part after `#`)
3. <http://localhost:3000> (Dagster), the **Assets** page, switched to the graph (lineage)
   view of all assets (the exact menu label differs per Dagster version: check it the day
   before)
4. <http://localhost:8502> (viewer)

Browser zoom about 125%, other applications closed, notifications off. Everything runs
locally: no internet needed during the demo.

## The five steps

### 1. The risk index (1½ min)

**Show:** the dashboard page *Risk index*, sector level *Sections*.
- Point at the counts: 5 of 19 sections above national; 1 rising.
- The chart: red above national in every quarter, blue below, the band per sector.
- Switch on **Final figures only** in the sidebar: the rising sector changes from
  *S Overige dienstverlening* to *B Delfstoffenwinning*. Switch it off again.

**Zeggen:** "Dit is wat de data-analist ziet: per sector het relatieve risico tegenover
landelijk, met de bandbreedte over acht kwartalen. Rood ligt in elk kwartaal boven
landelijk. Kijk wat er gebeurt als ik alleen definitieve cijfers toon: een andere sector
stijgt. Daarom moet een trend altijd met definitieve cijfers bevestigd worden."

### 2. One sector explained (1 min)

**Show:** under *One group in detail*, choose **Q Gezondheids- en welzijnszorg**.
- Read the explanation sentence aloud (40% more absence, stable, 88% run by natural
  persons).
- The chart below: the quarterly relative risk around 1.4, with the national line at 1.00.
- Optional, if time: page *Behind the numbers*, sector Q: what differs beyond the margin
  of error.

**Zeggen:** "Elk getal wordt in één zin uitgelegd, zodat de analist het met vertrouwen aan
de manager kan doorgeven. De lijn laat zien dat de zorg al twee jaar ongeveer 40% boven
landelijk ligt."

### 3. Where a number comes from (1 min)

**Show:** the lineage explorer tab, opened on `gold_risk_index.relative_risk`.
- Follow the chain to the left: gold `relative_risk` ← silver `fct_absence_rate.absence_pct`
  ← bronze `80072ned_typeddataset.Ziekteverzuimpercentage_1`.
- Click the bronze field: everything that depends on it appears on the right.

**Zeggen:** "Voor elke kolom is vastgelegd waar hij vandaan komt. Dit relatieve risico komt
uit één CBS-veld; omgekeerd zie ik meteen wat er allemaal verandert als het CBS dat veld
aanpast. Een test faalt zodra een nieuwe kolom geen herkomst heeft."

### 4. The workflow (1 min)

**Show:** Dagster, the asset lineage graph.
- The flow: `cbs_bronze` and `sbi_crosswalk` → `silver_cbs` → `gold`; `kvk_bronze` loaded by
  a person.
- Click `silver_cbs`: the checks (unique keys, keys in dimensions, one run).
- *Automation*: the monthly schedule; silver and gold only after new data and passed checks.

**Zeggen:** "Dagster haalt maandelijks nieuwe cijfers op. Is er niets nieuws, dan gebeurt
er niets. Een laag wordt pas gebouwd als de controles van de laag eronder zijn geslaagd, zo
bereiken foute gegevens het dashboard niet."

### 5. Data quality (½ min)

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
| The viewer's statistics take too long | show the screenshot; mention the 381,513 rows |
| The laptop or projector fails | present from the PDF of the slides and the report |

**Screenshots to make the day before** (one per step): the risk index page (with *Final
figures only* off and on), sector Q in detail, the lineage explorer on
`gold_risk_index.relative_risk`, the Dagster asset graph, and the viewer's data-quality
view of the KVK table.

## After the session

Stop the three terminals with Ctrl+C.
