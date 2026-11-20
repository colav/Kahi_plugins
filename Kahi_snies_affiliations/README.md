<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi SNIES affiliations plugin

Kahi uses this plugin to insert or update institutional affiliations from raw SNIES institution records.

# Description

This plugin reads raw institution records from a MongoDB collection populated from the SNIES/HECAA `Instituciones.xlsx` file and maps them into CoLav's standard `affiliations` entity.

The matching order is:

1. Exact match by `CÓDIGO_INSTITUCIÓN` as `external_ids.source = snies`.
2. Text and fuzzy name match against `affiliations.names.name`, following the approach used by other affiliations plugins.
3. Insert a new affiliation when no existing document is found.

Only standard affiliation fields are written. Raw descriptive fields that do not belong in the normalized entity, such as `MISIÓN`, are ignored.

Current mapping:

- `NOMBRE_INSTITUCIÓN` -> `names`, formatted as Spanish title case with `lang: es`; acronym-like suffixes are moved to `abbreviations`.
- `CÓDIGO_INSTITUCIÓN` -> `external_ids` with `source: snies` and `provenance: snies`.
- `NÚM_IDENTIFIC_TRIBUTARIA_NIT` -> `external_ids` with `source: nit` and
  `provenance: snies`, removing punctuation and the verification digit;
  malformed lengths, repeated digits and known placeholder values are ignored.
- `SECTOR` -> `types`, translating `Oficial` to `public` and `Privado` to `private`.
- `ESTADO` -> `status`, replacing any previous ROR status with `active` or `inactive`.
- `DEPARTAMENTO_DOMICILIO` and `MUNICIPIO_DOMICILIO` -> `addresses.state` and `addresses.city` only when those fields are missing.
- `PÁGINA_WEB` -> `external_urls` with `source: site` and `provenance: snies`.
- Accreditation fields -> `ranking` with `provenance: snies` and `sources: accreditation_status`.

# Installation

From this folder:

```shell
pip3 install .
```

From the package:

```shell
pip3 install Kahi_snies_affiliations
```

# Usage

Example workflow configuration:

```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  snies_affiliations:
    database_url: localhost:27017
    database_name: snies_institutions
    collection_name: institutions
    verbose: 1
```

Official sectionals (`CÓDIGO_IES_PADRE` different from
`CÓDIGO_INSTITUCIÓN`) are stored as compact objects in the principal
affiliation's `campuses` array. They are not inserted as separate affiliations.
Groups, authors and scientific products resolve a sectional to the principal
affiliation; the campus distinction is retained only as institutional metadata.

# License

BSD-3-Clause License

# Links

http://colav.udea.edu.co/
