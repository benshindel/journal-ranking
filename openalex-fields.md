# OpenAlex Work Object — Complete Field Reference

Every field available on an OpenAlex Work object, with type, measured compressed size per row, extraction status, and notes. Sizes measured from a 2,451-row Parquet sample with ZSTD compression.

---

## Top-Level Fields

| Field | Type | B/row | Extracting? | Notes |
|---|---|---|---|---|
| `id` | string | 6.1 | Yes (`work_id`) | OpenAlex ID, e.g. `https://openalex.org/W2741809807` |
| `doi` | string | 10.1 | Yes | Full DOI URL |
| `title` | string | 43.3 | Yes | Paper title. Largest single field by size. |
| `display_name` | string | — | No | Same as title |
| `publication_year` | int | 0.6 | Yes (`int16`) | |
| `publication_date` | string | 3.2 | Yes | ISO date string |
| `type` | string | 0.2 | Yes | OpenAlex normalized type (always "article" given our filter) |
| `type_crossref` | string | — | No | Removed in recent OpenAlex versions; use `primary_location.raw_type` instead |
| `language` | string | 0.3 | Yes | ISO 639-1 code |
| `indexed_in` | list[string] | 0.7 | Yes | e.g. `["crossref", "pubmed"]`. ~1.4 items avg |
| `is_paratext` | bool | 0.2 | Yes | Always False given our filter |
| `is_retracted` | bool | 0.2 | Yes | |
| `has_fulltext` | bool | 0.2 | Yes | 14% fill rate in test |
| `is_xpac` | bool | — | No | Extremely rare flag |
| `cited_by_count` | int | 0.7 | Yes (`int32`) | |
| `fwci` | float | 1.8 | Yes (`float32`) | Field-Weighted Citation Impact |
| `referenced_works_count` | int | 0.8 | Yes (`int16`) | Key filter field (>=10) |
| `countries_distinct_count` | int | 0.5 | Yes (`int16`) | |
| `institutions_distinct_count` | int | 0.7 | Yes (`int16`) | |
| `locations_count` | int | 0.4 | Yes (`int16`) | 0 = suspicious |
| `corresponding_author_ids` | list[string] | — | No | Author IDs, not needed |
| `corresponding_institution_ids` | list[string] | 4.0 | Yes | Institution IDs of corresponding authors |
| `referenced_works` | list[string] | ~100+ | No | Full list of cited work IDs. Very large. |
| `related_works` | list[string] | ~50+ | No | Algorithmically related. Large. |
| `abstract_inverted_index` | object | ~500+ | No | Full abstract in inverted index format. Very large. |
| `created_date` | string | ~3 | No | When record was created in OpenAlex |
| `updated_date` | string | ~3 | No | When record was last updated |

## `ids` Object

| Field | Type | B/row | Extracting? | Notes |
|---|---|---|---|---|
| `ids.openalex` | string | — | No | Same as `id` |
| `ids.doi` | string | — | No | Same as `doi` |
| `ids.mag` | string | — | No | Legacy Microsoft Academic Graph ID |
| `ids.pmid` | string | 1.1 | Yes (`ids_pmid`) | PubMed ID. 13% fill rate. Strong biomedical signal. |
| `ids.pmcid` | string | — | No | PubMed Central ID. Could add but low value. |

## `primary_location` Object

| Field | Type | B/row | Extracting? | Notes |
|---|---|---|---|---|
| `primary_location.source.id` | string | 1.2 | Yes (`source_id`) | Journal/source OpenAlex ID |
| `primary_location.source.display_name` | string | 2.2 | Yes (`source_name`) | Journal name |
| `primary_location.source.issn_l` | string | 1.0 | Yes (`source_issn_l`) | Linking ISSN |
| `primary_location.source.type` | string | 0.4 | Yes (`source_type`) | "journal", "repository", etc. |
| `primary_location.source.host_organization_name` | string | 1.0 | Yes (`publisher`) | Publisher name |
| `primary_location.version` | string | 0.4 | Yes (`source_version`) | e.g. "publishedVersion". 99% fill rate. |
| `primary_location.raw_type` | string | 0.5 | Yes (`location_raw_type`) | Crossref raw type. 100% fill rate. |
| `primary_location.is_oa` | bool | — | No | Redundant with `open_access.is_oa` |
| `primary_location.landing_page_url` | string | — | No | URL to paper. Large, not needed. |
| `primary_location.pdf_url` | string | — | No | URL to PDF. Often null. |
| `primary_location.license` | string | — | No | e.g. "cc-by". Captured by oa_status. |
| `primary_location.license_id` | string | — | No | |
| `primary_location.is_accepted` | bool | — | No | |
| `primary_location.is_published` | bool | — | No | |
| `primary_location.raw_source_name` | string | — | No | Raw name before normalization |

## `biblio` Object

| Field | Type | B/row | Extracting? | Notes |
|---|---|---|---|---|
| `biblio.volume` | string | 0.8 | Yes (`biblio_volume`) | 50% fill rate |
| `biblio.issue` | string | 0.7 | Yes (`biblio_issue`) | |
| `biblio.first_page` | string | 2.5 | Yes (`biblio_first_page`) | |
| `biblio.last_page` | string | 2.5 | Yes (`biblio_last_page`) | |

## `citation_normalized_percentile` Object

| Field | Type | B/row | Extracting? | Notes |
|---|---|---|---|---|
| `citation_normalized_percentile.value` | float | 2.6 | Yes (`citation_percentile`) | |
| `citation_normalized_percentile.is_in_top_1_percent` | bool | 0.2 | Yes (`is_top_1_pct`) | |
| `citation_normalized_percentile.is_in_top_10_percent` | bool | 0.3 | Yes (`is_top_10_pct`) | |

## `open_access` Object

| Field | Type | B/row | Extracting? | Notes |
|---|---|---|---|---|
| `open_access.is_oa` | bool | 0.2 | Yes (`is_oa`) | |
| `open_access.oa_status` | string | 0.5 | Yes (`oa_status`) | "gold", "green", "hybrid", "bronze", "closed" |

## `apc_list` / `apc_paid` Objects

| Field | Type | B/row | Extracting? | Notes |
|---|---|---|---|---|
| `apc_list.value_usd` | int | 0.4 | Yes (`apc_list_usd`) | List price APC in USD |
| `apc_paid.value_usd` | int | 0.4 | Yes (`apc_paid_usd`) | Actual APC paid in USD |

## `primary_topic` Object

| Field | Type | B/row | Extracting? | Notes |
|---|---|---|---|---|
| `primary_topic.id` | string | — | No | Topic ID (we extract field/subfield/domain instead) |
| `primary_topic.display_name` | string | — | No | Topic name |
| `primary_topic.score` | float | 0.9 | Yes (`topic_score`) | Confidence score |
| `primary_topic.field.id` | string | 0.6 | Yes (`topic_field_id`) | |
| `primary_topic.field.display_name` | string | 0.7 | Yes (`topic_field_name`) | 27 possible fields |
| `primary_topic.subfield.id` | string | 0.9 | Yes (`topic_subfield_id`) | |
| `primary_topic.subfield.display_name` | string | 1.5 | Yes (`topic_subfield_name`) | ~250 subfields |
| `primary_topic.domain.id` | string | 0.4 | Yes (`topic_domain_id`) | |
| `primary_topic.domain.display_name` | string | 0.4 | Yes (`topic_domain_name`) | 5 domains |

## `topics` Array

| Field | Type | B/row | Extracting? | Notes |
|---|---|---|---|---|
| `topics[].id` | string | 3.6 | Yes (`topic_ids`) | All topic IDs. ~1.4 items avg. |
| `topics[].display_name` | string | 10.5 | Yes (`topic_names`) | All topic names. Largest list field. |
| `topics[].score` | float | 3.0 | Yes (`topic_scores`) | All topic scores. |
| `topics[].subfield.*` | various | — | No | Redundant — primary_topic captures top hit |
| `topics[].field.*` | various | — | No | Same |
| `topics[].domain.*` | various | — | No | Same |

## `authorships` Array

Per-author fields. We extract aggregated data (not per-author).

| Field | Type | B/row | Extracting? | Notes |
|---|---|---|---|---|
| `authorships[].author_position` | string | — | Implicitly | Used to identify first/last author |
| `authorships[].author.id` | string | — | No | Author OpenAlex ID |
| `authorships[].author.display_name` | string | 9.0 / 6.3 | Yes (first + last only) | `first_author_name`, `last_author_name` |
| `authorships[].author.orcid` | string | — | No | ORCID identifier |
| `authorships[].raw_author_name` | string | — | No | Name as printed on paper |
| `authorships[].is_corresponding` | bool | — | Implicitly | Used via `corresponding_institution_ids` |
| `authorships[].countries` | list[string] | — | No | Per-author country codes |
| `authorships[].institutions[].id` | string | 7.1 | Yes (`institution_ids`, deduplicated) | All unique institution IDs across all authors |
| `authorships[].institutions[].display_name` | string | 14.1 | Yes (`institution_names`, deduplicated) | Human-readable names |
| `authorships[].institutions[].ror` | string | — | No | ROR identifier |
| `authorships[].institutions[].country_code` | string | 1.2 | Yes (`institution_country_codes`, deduplicated) | |
| `authorships[].institutions[].type` | string | 0.9 | Yes (`institution_types`, deduplicated) | "education", "company", "government", etc. |
| `authorships[].institutions[].lineage` | list[string] | 8.1 | Yes (`institution_lineage_ids`, flattened) | **Critical for tier matching** — parent institution chain |
| `authorships[].raw_affiliation_strings` | list[string] | — | No | Raw text from paper. Large. |
| `authorships[].affiliations[].raw_affiliation_string` | string | — | No | Same, structured |
| `authorships[].affiliations[].institution_ids` | list[string] | — | No | Redundant with institutions[] |

## Other Arrays (Not Extracted)

| Field | Type | Est. B/row | Why skipped |
|---|---|---|---|
| `locations[]` | array of location objects | ~20+ | primary_location suffices |
| `best_oa_location` | object | ~10 | Not needed for ranking |
| `concepts[]` | array | ~20+ | Deprecated, replaced by topics |
| `mesh[]` | array | ~15+ | Biomedical only; topics cover this |
| `keywords[]` | array | ~10+ | Author-assigned; noisy |
| `sustainable_development_goals[]` | array | ~5 | Not relevant |
| `grants[]` / `funders[]` | array | ~10+ | Removed from schema (user request) |
| `awards[]` | array | ~5 | Rare |
| `counts_by_year[]` | array | ~20+ | Citation time series; not needed |
| `cited_by_percentile_year` | object | ~5 | Redundant with citation_normalized_percentile |

---

## Schema Summary

| | Old Schema | New Schema |
|---|---|---|
| Fields | 37 | 57 |
| Compressed bytes/row | 58 | 165 |
| Estimated full dataset | 5-9 GB | 13-25 GB |
