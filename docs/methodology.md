# Methodology

## Overview

We rank academic journals by measuring how often researchers at elite institutions publish in them. The intuition is that top researchers have their pick of journals, so the journals they choose to publish in reveal something about journal quality that citation-based metrics don't capture. Rather than measuring how often a journal's papers are cited, we measure who chooses to publish there.

## Data Source

- **OpenAlex** academic database (open, free, comprehensive)
- Data obtained from the S3 bulk data snapshot (not the API)
- Currently using a partial sample (~5% by bytes) for development; full dataset extraction is in progress
- Coverage: articles published 2015--2024

## Filtering

We include works meeting **all** of the following criteria:

- OpenAlex type = `article`
- Not flagged as paratext
- At least 10 cited references (`referenced_works_count >= 10`)

The reference count threshold filters out non-research content---news articles, editorials, letters to the editor, conference abstracts---that OpenAlex classifies as type "article." These items typically cite few or no references and would add noise to journal-level statistics.

## Ranking Metric

For each journal, we calculate two rates:

- **Tier 1 rate** = (papers with at least one Tier 1 institution author) / (papers with at least one identified institution)
- **Tier 2 rate** = (papers with at least one Tier 1 or Tier 2 institution author) / (papers with at least one identified institution)

The denominator uses only papers where OpenAlex identified at least one institutional affiliation, since papers without any institution data cannot contribute signal about institutional representation.

Papers are not double-counted: a paper with authors from both Harvard and MIT counts once toward the Tier 1 numerator.

## Institution Tiers

**Tier 1** (4 institutions):

- Harvard University
- Stanford University
- MIT
- University of Oxford

**Tier 2** (23 institutions total, inclusive of Tier 1):

Tier 1 plus:

- Google
- Meta
- Microsoft
- NIH
- US DHHS
- Chinese Academy of Sciences
- ETH Zurich
- UCL
- Imperial College London
- University of Toronto
- UC Berkeley
- Princeton University
- Johns Hopkins University
- National University of Singapore
- University of Cambridge
- Tsinghua University
- University of Tokyo
- Max Planck Society
- CNRS

Institution matching uses OpenAlex institution IDs. For most institutions, we expand to include known sub-institutions (e.g., papers listing "Harvard Medical School" are counted under Harvard University). For very large umbrella organizations---Chinese Academy of Sciences, CNRS, Max Planck Society---only the parent organization ID is matched, since these entities have hundreds of sub-institutions that would require extensive manual curation.

## Field Classification

Each journal is assigned a primary academic field based on the most common OpenAlex topic field among its papers. A secondary field is listed when a second field represents more than 25% of the journal's papers.

## Limitations

- **Partial data.** Currently based on roughly 5% of the full OpenAlex snapshot. Rankings may shift when computed over the complete dataset.
- **Institution coverage.** Not all papers have institutional affiliation data in OpenAlex. Coverage is higher for Crossref-indexed journals (~76%) than the overall average (~52%), so journals outside Crossref may be underrepresented.
- **Reference threshold.** The 10-reference minimum may exclude some legitimate short research papers, particularly in mathematics and certain engineering subfields where fewer citations are conventional.
- **Sub-institution matching.** Expansion to sub-institutions is incomplete for some large umbrella organizations, potentially undercounting their output.
- **Tier selection.** The choice of tier institutions is subjective and reflects a particular view of research excellence weighted toward English-speaking and Western institutions.
