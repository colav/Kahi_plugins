<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi lens patents plugin
Adds the Lens ID, the Lens URL and the Lens abstracts to the patents that are
already in Kahi. It never inserts patents that exist only in Lens.

# Description
Runs after `minciencias_opendata_patents`. It reads the Lens records loaded by
impactu_airflow (`scripts/load_lens_patents.py`, collection `lens.patents`, one
document per lens_id with the full Lens record under `lens`) and the snapshot
of the last complete `minciencias_opendata_patents` run (registration numbers,
countries and years, which Kahi patents do not keep).

Matching rules, in order (`matched_by` in the audit collection):

| Rule | Condition |
|---|---|
| `number` | registration number equals the publication, application or priority number of a Lens record (`NC2023/0017292` = `CO2023017292`, `PCT/IB2019/053356`, `US 7,041,647`, kind codes ignored) |
| `family` | registration number equals a member of the record's simple family |
| `title_exact` | normalized title equals a Lens title in any language; ambiguous across families is skipped |
| `title_fuzzy` | `token_set_ratio >= 92`, year +-3, half of the content words shared |
| `inventor_title` | an author shares two name tokens with an inventor, `token_set_ratio >= 85`, year +-3 |

Among records of the same invention the one in the patent's jurisdiction is
preferred, then the Colombian one, then one with an abstract and the closest
year. Generic titles ("Patente de modelo de utilidad", "Certificado de
Obtentor") never match by title.

Each matched patent gets:

- `external_ids`: `{provenance: lens, source: lens, id: <lens_id>}`
- `external_urls`: `{provenance: lens, source: lens, url: https://www.lens.org/lens/patent/<lens_id>}`
- `abstracts`: one entry per language, inverted index, `source: lens`
- `updated`: `{source: lens, time}` (only when something changed)

A patent that no longer matches loses its Lens entries. Every decision is
written to `lens_patents_matches` (one document per patent).

Lens data is non commercial (CC BY-NC-SA like terms): whoever shows it must
display the Lens logo and a link to lens.org (https://about.lens.org/policies/#attribution).

# Merging duplicated patents

MinCiencias publishes a patent once per CvLAC that declares it (each with its
owner as the only author) and once per official measurement record (no authors).
With `merge_duplicates: true` the patents that matched the same Lens record
become one when there is hard evidence that they are the same patent:

- the patent matched Lens by number or family (`number`, `family`);
- one of its authors is an inventor of the Lens record (`lens_inventor`);
- it shares an inventor with another patent of the group (`shared_inventor`);
- its owner (COD_RH of the official record) is an author or the owner of another
  patent of the group (`shared_owner`).

A title-only match with none of these stays apart. The survivor (matched by
number or family, then most authors) absorbs authors, groups, identifiers,
titles, types and ranking; the others are deleted and stored whole in
`lens_patents_merges` (`merged_into`, `evidence`, `document`), so a merge can be
undone. `merge_dry_run: true` deletes nothing and writes
`lens_patents_merges_dry_run`. Importing `minciencias_opendata_patents` again
restores the deleted patents; run this plugin after it.

On the November 2026 cut: 6 847 patents, 4 565 matched, 1 944 merged away
(4 903 remain); 132 patents still share 60 Lens records without evidence.

# Usage
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
workflow:
  lens_patents:
    database_url: localhost:27017
    database_name: lens
    collection_name: patents
    snapshot_database_name: dam
    merge_duplicates: false
    merge_dry_run: false
    verbose: 1
```

# License
BSD-3-Clause License

# Links
http://colav.udea.edu.co/
