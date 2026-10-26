# Q&A practice for the presentation

Questions an examiner is likely to ask after the demo, with a short answer and where the
evidence is. Practise them **in your own words**: the point is that you can explain every
choice yourself, not recite these lines. The answers (*Antwoord*) are in Dutch, the
language of the presentation; the evidence links point into the repository.

## The model

**1. Waarom geen machine learning?**
*Antwoord:* De vraag is beschrijvend (waar is het verzuim hoger, en is dat stabiel?), er
zijn maar 26 kwartalen voor zo'n 38 groepen, en de analist moet elk getal aan een manager
kunnen uitleggen. Een white-box-model van rekenregels is met de hand na te rekenen; de
analist trekt de conclusie, niet het model.
*Evidence:* report 3.3; [gold's README](../pipeline/gold/README.md#phase-4--model-design-and-testing-the-risk-index).

**2. Hoe weet je dat de index klopt, en dat hij iets zegt?**
*Antwoord:* Twee dingen. Tests op gegevens met een vooraf bekend antwoord bewijzen dat de
berekening klopt. En een terugtoets: voor 13 eerdere kwartalen is de index gebouwd met
alleen de cijfers tot dat moment. Een sector die *hoger* of *lager* scoorde, lag een jaar
later in alle 440 gevallen nog aan dezelfde kant van landelijk. Kanttekening: de
meetmomenten overlappen, dus 100% overschat het bewijs wat.
*Evidence:* `python -m tools.backtest_risk`; gold's README, "Backtest".

**3. Wat zegt de trend dan?**
*Antwoord:* Wat er de afgelopen twee jaar gebeurde, niet wat er gaat gebeuren. In de
terugtoets steeg een *stijgende* sector het jaar daarna niet vaker verder dan een stabiele
(44% tegen 45%). Daarom zegt het dashboard: een trend is geen voorspelling. Bij pure ruis
zou je zelfs een terugval zien (terugkeer naar het gemiddelde); de echte cijfers doen het
iets beter dan ruis, maar niet genoeg om te voorspellen.
*Evidence:* gold's README, "Backtest"; `tests/test_backtest.py`.

**4. Waarom acht kwartalen, minimaal vier, en een drempel van 0,05 per jaar?**
*Antwoord:* Acht kwartalen = twee jaar: genoeg punten voor een bandbreedte, en recent genoeg
om over nu te gaan. Minimaal vier, omdat het relatieve risico een gemiddelde over een heel
jaar is (dan vallen seizoenen weg). 0,05 per jaar is 5% van het landelijke niveau per jaar,
over twee jaar 0,10: een verschil dat een analist zou willen zien. Het zijn instellingen
in `config.yaml`, geen natuurconstanten. Een t-toets op de helling zou vandaag één oordeel
veranderen; omdat de trend beschrijvend is, is die niet toegevoegd.
*Evidence:* `config.yaml` (`gold.risk_index`); gold's README.

**5. Is de bandbreedte een foutmarge?**
*Antwoord:* Nee. Het CBS geeft voor tabel 80072NED geen foutmarge. De band is het laagste
en hoogste kwartaal van de laatste acht: hij laat zien hoe stabiel een sector is. Echte
95%-intervallen zitten alleen in de enquête (86009NED), op de pagina *Behind the numbers*.

## Data engineering

**6. Waarom Delta Lake via delta-rs, en geen Spark?**
*Antwoord:* De data is klein (de hele lakehouse ongeveer 260 MB). Delta Lake geeft
transacties, versies (*time travel*) en een open formaat; delta-rs doet dat in Python,
zonder cluster of databaseserver. Spark is voor clusters en terabytes; hier zou het alleen
opstarttijd en geheugen kosten. In de cloud kan dezelfde opslag blijven.
*Evidence:* report 4.1 (tools table); `docs/cloud_design.md`.

**7. Wat gebeurt er als het CBS een cijfer herziet?**
*Antwoord:* Elk verzoek heeft een sleutel met de periode én de CBS-status (voorlopig of
definitief). Een sleutel die al in het log staat, wordt nooit opnieuw opgevraagd. Bij een
herziening verandert de status, dus de sleutel: alleen die periode wordt opnieuw opgehaald,
en de oude rijen worden vervangen, niet aangevuld.
*Evidence:* [bronze's README](../pipeline/bronze/README.md#design-decisions); tests in
`tests/test_delta.py`.

**8. Hoe voorkom je dat foute data het dashboard bereikt?**
*Antwoord:* Op drie plekken: elke CBS-batch wordt vóór het schrijven gecontroleerd (een
afgekeurde batch gaat in quarantaine); silver en gold worden eerst in een tijdelijke map
gebouwd en pas samen gepubliceerd; en Dagster bouwt een laag pas als de blokkerende
controles van de laag eronder geslaagd zijn.

**9. Er zitten 381.513 identieke rijen in het KVK-bestand. Waarom niet verwijderd?**
*Antwoord:* Het open bestand heeft geen KVK-nummer, dus ik kan niet vaststellen of het
dubbelingen zijn of verschillende bedrijven met dezelfde kenmerken. Ik heb ze gemarkeerd,
niet verwijderd: geen aannames die de gebruiker niet kan zien.

**10. Hoe koppel je KVK (SBI 2025) aan het CBS (SBI 2008)?**
*Antwoord:* Via de officiële schakeltabel van het CBS. Waar die niet eenduidig is (één code
naar meerdere), wordt het geval gemarkeerd, niet geraden. Bij 90% van de actieve bedrijven
is de sector eenduidig.

**11. Schaalt dit?**
*Antwoord:* Het deel dat groeit is KVK: elke maand een momentopname van 1,9 miljoen rijen.
Silver KVK verwerkt alleen nieuwe momentopnamen, één tegelijk, zodat het geheugen niet
meegroeit (gemeten op 4 gesimuleerde momentopnamen: 1,65 GB voor een volledige herbouw). Bij veel meer data
zijn Polars of de cloud de volgende stap.
*Evidence:* README, section "Silver"; TODO.md, "KVK: incremental updates" (measurements).

**12. Een reviewer vond een mogelijke fout in de koppeling met de bedrijfsaantallen. Wat was het?**
*Antwoord:* De aantallen werden op SBI-code gekoppeld; een samengestelde sector heeft geen
code, dus die zouden hetzelfde, verkeerde aantal kunnen krijgen. In de echte cijfers kwam
dat niet voor, maar het is gerepareerd (koppelen op de sectorsleutel) met een test die
op de oude code faalt. Een goed voorbeeld van waarom een tweede blik nuttig is.

## Business value and responsibility

**13. Wat moet de verzekeraar hier concreet mee?**
*Antwoord:* De analist ziet per sector en grootteklasse waar het verzuim structureel hoger
ligt, met uitleg in één zin. Via de manager gaat dat naar risicomanagement (portefeuille,
premies) en accountmanagers (preventiegesprekken met werkgevers). Als benchmark náást de
eigen cijfers, op groepsniveau.

**14. Mag je een sectorcijfer gebruiken voor één werkgever of voor premies?**
*Antwoord:* Niet voor één werkgever: dat is een groepscijfer (*ecological fallacy*). Voor
premies pas na een toets door compliance: sector kan indirect een beschermde groep raken
(de zorg heeft vooral vrouwelijke werknemers).

**15. Zitten er persoonsgegevens in?**
*Antwoord:* Niet van verzekerden. Wel is een bedrijf op rijniveau soms herleidbaar (851.804
van de 1,9 miljoen bedrijven zijn uniek op regio, SBI-code en startdatum), dus het dashboard
toont alleen aantallen, met kleine groepen verborgen. En het log bevat wie een KVK-bestand
laadde; die naam wordt na een jaar verwijderd.
*Evidence:* `docs/privacy_and_risks.md`.

## Your own work and GenAI

**16. Wat heb je zelf gedaan, en wat de AI?**
*Antwoord (in je eigen woorden):* Ik nam de beslissingen en de AI-assistent bouwde, testte
en documenteerde binnen die keuzes. Noem twee of drie beslissingen die van jou kwamen, bijv.
markeren in plaats van aannemen, de verzekeraar en de keten analist → manager →
risicomanagement, en de vraag of pandas zou schalen, die tot de incrementele opzet leidde.
*Evidence:* `docs/genai_use.md`, section 3 (the author's decisions).

**17. Hoe weet je dat de code van de AI klopt?**
*Antwoord:* Niet op vertrouwen: 396 tests, ook op een schone machine in GitHub Actions; code
expres kapotgemaakt om te zien of de tests dat merken; resultaten vergeleken op de echte
data na elke grote wijziging; en vragen als "hoe heb je dat getest?", die een onterecht
afgevinkt item aan het licht brachten.
*Evidence:* `docs/genai_use.md`, sections 4 and 5.

**18. Wat zou je anders doen?**
*Antwoord (in je eigen woorden):* bijvoorbeeld eerder een terugtoets van het model doen, of
eerder iemand anders laten meekijken (de review vond een echte, latente fout).
