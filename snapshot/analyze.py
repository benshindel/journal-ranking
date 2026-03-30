"""
Journal Ranking Analysis
========================
Loads extracted OpenAlex parquet data, computes institution-based
journal rankings, and outputs ranking_data.json for the website.

Usage:
    python3 analyze.py                          # Run with defaults
    python3 analyze.py --output ../ranking.json  # Custom output path
    python3 analyze.py --min-papers 100          # Require 100+ papers
    python3 analyze.py --min-refs 20             # Require 20+ references
"""

import argparse
import json
import time
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Institution tier definitions with expanded sub-institution IDs
# Looked up via OpenAlex API: filter=lineage:{ID}&per_page=200
# Only includes children with works_count >= 100
# Last updated: 2026-03-29
# ---------------------------------------------------------------------------

# Tier 1: 4 elite institutions
TIER1_EXPANSION = {
    "https://openalex.org/I136199984": {  # Harvard University
        "https://openalex.org/I136199984",
        "https://openalex.org/I2801851002",  # Harvard University Press
        "https://openalex.org/I4210124175",  # Center for Astrophysics Harvard & Smithsonian
        "https://openalex.org/I4210155419",  # Center for Systems Biology
        "https://openalex.org/I4210134579",  # Dana-Farber/Harvard Cancer Center
        "https://openalex.org/I4210127055",  # Martinos Center for Biomedical Imaging
        "https://openalex.org/I4210157861",  # Harvard Stem Cell Institute
        "https://openalex.org/I153167508",   # Smithsonian Astrophysical Observatory
        "https://openalex.org/I2802422659",  # Ragon Institute of MGH, MIT and Harvard
        "https://openalex.org/I4210106258",  # Harvard College Observatory
        "https://openalex.org/I4210143899",  # Center for Vascular Biology Research
        "https://openalex.org/I4210087356",  # Gordon Center for Medical Imaging
        "https://openalex.org/I4210133893",  # Berenson Allen Center for Noninvasive Brain Stimulation
        "https://openalex.org/I4210159271",  # MIT-Harvard Center for Ultracold Atoms
        "https://openalex.org/I44142251",    # Wyss Institute for Biologically Inspired Engineering
    },
    "https://openalex.org/I97018004": {  # Stanford University
        "https://openalex.org/I97018004",
        "https://openalex.org/I4210137306",  # Stanford Medicine
        "https://openalex.org/I2801935854",  # SLAC National Accelerator Laboratory
        "https://openalex.org/I4210120900",  # Stanford Synchrotron Radiation Lightsource
        "https://openalex.org/I4210094059",  # Kavli Institute for Particle Astrophysics and Cosmology
        "https://openalex.org/I1334016132",  # Lucile Packard Children's Hospital
        "https://openalex.org/I4210136624",  # Linac Coherent Light Source
        "https://openalex.org/I4390039303",  # Stanford Cancer Institute
        "https://openalex.org/I4210110647",  # Children's Hospital Central California
        "https://openalex.org/I4210100286",  # Chan Zuckerberg Biohub San Francisco
        "https://openalex.org/I4392738099",  # Stanford SystemX Alliance
    },
    "https://openalex.org/I63966007": {  # MIT
        "https://openalex.org/I63966007",
        "https://openalex.org/I4210122954",  # MIT Lincoln Laboratory
        "https://openalex.org/I4210127055",  # Martinos Center for Biomedical Imaging
        "https://openalex.org/I4210157710",  # Whitehead Institute for Biomedical Research
        "https://openalex.org/I144112489",   # McGovern Institute for Brain Research
        "https://openalex.org/I2802422659",  # Ragon Institute of MGH, MIT and Harvard
        "https://openalex.org/I4210159271",  # MIT-Harvard Center for Ultracold Atoms
        "https://openalex.org/I4390039253",  # Koch Institute for Integrative Cancer Research
        "https://openalex.org/I4210109539",  # Institute for Soldier Nanotechnologies
        "https://openalex.org/I4210125891",  # MIT Sea Grant
    },
    "https://openalex.org/I40120149": {  # University of Oxford
        "https://openalex.org/I40120149",
        "https://openalex.org/I4210126604",  # Mahidol Oxford Tropical Medicine Research Unit
        "https://openalex.org/I1336263701",  # Centre for Human Genetics
        "https://openalex.org/I4210101881",  # Wellcome Centre for Integrative Neuroimaging
        "https://openalex.org/I2802626578",  # Oxford University Press
        "https://openalex.org/I4210150114",  # Oxford University Clinical Research Unit
        "https://openalex.org/I4210129944",  # Oxford Centre for Diabetes
        "https://openalex.org/I4210110594",  # MRC Weatherall Institute of Molecular Medicine
        "https://openalex.org/I4210124851",  # MRC Human Immunology Unit
        "https://openalex.org/I4210162189",  # Wellcome Centre for Ethics and Humanities
        "https://openalex.org/I4210106471",  # CRUK/MRC Oxford Institute for Radiation Oncology
        "https://openalex.org/I4210095040",  # Shoklo Malaria Research Unit
        "https://openalex.org/I4210100268",  # Medawar Building for Pathogen Research
        "https://openalex.org/I4210105326",  # Myanmar Oxford Clinical Research Unit
        "https://openalex.org/I4210147017",  # Centre for Observation and Modelling of Earthquakes
        "https://openalex.org/I4210141788",  # Lao-Oxford-Mahosot Hospital-Wellcome Trust
        "https://openalex.org/I4210108304",  # MRC Brain Network Dynamics Unit
        "https://openalex.org/I4210158028",  # Cancer Research UK Oxford Centre
        "https://openalex.org/I4210116389",  # Cambodia-Oxford Medical Research Unit
        "https://openalex.org/I4389425243",  # Oxford University Clinical Research Unit Indonesia
    },
}

# Tier 2 additional: 19 more institutions (Tier 2 = Tier 1 + these)
TIER2_ADDITIONAL_EXPANSION = {
    "https://openalex.org/I1291425158": {  # Google
        "https://openalex.org/I1291425158",
        "https://openalex.org/I4210100430",  # Google (Switzerland)
        "https://openalex.org/I4210113297",  # Google (United Kingdom)
        "https://openalex.org/I4210117425",  # Google (Israel)
        "https://openalex.org/I4210148186",  # Google (Canada)
    },
    "https://openalex.org/I4210114444": {  # Meta
        "https://openalex.org/I4210114444",
        "https://openalex.org/I2252078561",  # Meta (Israel)
        "https://openalex.org/I4210111288",  # Meta (United Kingdom)
    },
    "https://openalex.org/I1290206253": {  # Microsoft
        "https://openalex.org/I1290206253",
        "https://openalex.org/I4210164937",  # Microsoft Research (UK)
        "https://openalex.org/I4210113369",  # Microsoft Research Asia (China)
        "https://openalex.org/I4210086099",  # Microsoft (Brazil)
        "https://openalex.org/I4210124949",  # Microsoft Research (India)
        "https://openalex.org/I4210105678",  # Microsoft (Finland)
        "https://openalex.org/I1316064682",  # LinkedIn
        "https://openalex.org/I4400600948",  # Microsoft Research New England
        "https://openalex.org/I4210087053",  # Microsoft (Germany)
        "https://openalex.org/I4210162141",  # Microsoft (India)
        "https://openalex.org/I4210125051",  # Microsoft (Israel)
        "https://openalex.org/I4401726785",  # Microsoft Research NYC
        "https://openalex.org/I4210153468",  # Microsoft (Canada)
        "https://openalex.org/I4210108625",  # Microsoft (UK)
        "https://openalex.org/I4402554038",  # Microsoft Research Montreal
    },
    "https://openalex.org/I1299303238": {  # NIH
        "https://openalex.org/I1299303238",
        "https://openalex.org/I4210140884",  # National Cancer Institute
        "https://openalex.org/I4210134534",  # NIAID
        "https://openalex.org/I4210106489",  # NHLBI
        "https://openalex.org/I4210149717",  # Center for Cancer Research
        "https://openalex.org/I4210144228",  # NICHD
        "https://openalex.org/I4210090567",  # NIDDK
        "https://openalex.org/I4210095140",  # NIEHS
        "https://openalex.org/I4210158500",  # NIMH
        "https://openalex.org/I4210110767",  # NINDS
        "https://openalex.org/I4210136897",  # NIA
        "https://openalex.org/I4210130649",  # Frederick National Laboratory for Cancer Research
        "https://openalex.org/I4210155647",  # NIH Clinical Center
        "https://openalex.org/I4210090236",  # NHGRI
        "https://openalex.org/I1327069482",  # NIDA
        "https://openalex.org/I4210087962",  # NIAMS
        "https://openalex.org/I4210139686",  # NEI
        "https://openalex.org/I4210088259",  # NIDCR
        "https://openalex.org/I4210121815",  # NIAAA
        "https://openalex.org/I4210109390",  # NCBI
        "https://openalex.org/I2800548410",  # NLM
        "https://openalex.org/I4210148682",  # NCATS
        "https://openalex.org/I874236823",   # Fogarty International Center
    },
    "https://openalex.org/I1299022934": {  # US DHHS (includes NIH, CDC, FDA, etc.)
        "https://openalex.org/I1299022934",
        "https://openalex.org/I1289490764",  # CDC
        "https://openalex.org/I1320320070",  # FDA
        "https://openalex.org/I35344726",    # US Public Health Service
        "https://openalex.org/I198423848",   # NIOSH
        "https://openalex.org/I1333606569",  # Center for Drug Evaluation and Research
        "https://openalex.org/I1318287680",  # Center for Biologics Evaluation and Research
        "https://openalex.org/I1304557061",  # National Center for Toxicological Research
        "https://openalex.org/I82994568",    # AHRQ
        "https://openalex.org/I50959184",    # CMS
        # Note: NIH and all its sub-institutes are already in the NIH entry above
    },
    # CAS, CNRS, Max Planck: parent ID only (too many sub-institutions)
    "https://openalex.org/I19820366": {  # Chinese Academy of Sciences
        "https://openalex.org/I19820366",
    },
    "https://openalex.org/I33213144": {  # CNRS
        "https://openalex.org/I33213144",
    },
    "https://openalex.org/I149899117": {  # Max Planck Society
        "https://openalex.org/I149899117",
    },
    "https://openalex.org/I35440088": {  # ETH Zurich
        "https://openalex.org/I35440088",
        "https://openalex.org/I4210090941",  # Institute for Biomedical Engineering
        "https://openalex.org/I4210122261",  # Swiss Data Science Center
        "https://openalex.org/I4210133264",  # Collegium Helveticum
    },
    "https://openalex.org/I45129253": {  # UCL
        "https://openalex.org/I45129253",
        "https://openalex.org/I4210091428",  # MRC Clinical Trials Unit at UCL
        "https://openalex.org/I165524941",   # Wellcome Centre for Human Neuroimaging
        "https://openalex.org/I4210157240",  # Institute of Structural and Molecular Biology
        "https://openalex.org/I4210093127",  # MRC Laboratory for Molecular Cell Biology
        "https://openalex.org/I4210138523",  # UCL Biomedical Research Centre
        "https://openalex.org/I4210139815",  # MRC Unit for Lifelong Health and Ageing
        "https://openalex.org/I4210134576",  # MRC Prion Unit
        "https://openalex.org/I4210122016",  # Wellcome/EPSRC Centre for Interventional and Surgical Sciences
        "https://openalex.org/I4396570676",  # Sainsbury Wellcome Centre
    },
    "https://openalex.org/I47508984": {  # Imperial College London
        "https://openalex.org/I47508984",
        "https://openalex.org/I4210161592",  # MRC London Institute of Medical Sciences
        "https://openalex.org/I4392738229",  # MRC Centre for Environment and Health
    },
    "https://openalex.org/I185261750": {  # University of Toronto
        "https://openalex.org/I185261750",
        "https://openalex.org/I4391768120",  # Sunnybrook Research Institute
        "https://openalex.org/I4210110940",  # Canadian Institute for Theoretical Astrophysics
        "https://openalex.org/I4210091032",  # Ted Rogers Centre for Heart Research
        "https://openalex.org/I9267121",     # Fields Institute
    },
    "https://openalex.org/I95457486": {  # UC Berkeley
        "https://openalex.org/I95457486",
        "https://openalex.org/I4210114105",  # Tsinghua-Berkeley Shenzhen Institute
        "https://openalex.org/I103922791",   # QB3
        "https://openalex.org/I4210109258",  # Innovative Genomics Institute
        "https://openalex.org/I4210144282",  # Plant Gene Expression Center
        "https://openalex.org/I4210100286",  # Chan Zuckerberg Biohub SF
    },
    "https://openalex.org/I20089843": {  # Princeton
        "https://openalex.org/I20089843",
        "https://openalex.org/I2799567181",  # Princeton Plasma Physics Laboratory
        "https://openalex.org/I2799411422",  # Center for Discrete Mathematics and TCS
    },
    "https://openalex.org/I145311948": {  # Johns Hopkins
        "https://openalex.org/I145311948",
        "https://openalex.org/I2802946424",  # JHU Applied Physics Laboratory
        "https://openalex.org/I4210092215",  # JHU Berman Institute of Bioethics
        "https://openalex.org/I4389425327",  # JHU Center for AIDS Research
        "https://openalex.org/I4210131961",  # JHU SAIS Bologna Center
    },
    "https://openalex.org/I165932596": {  # NUS
        "https://openalex.org/I165932596",
        "https://openalex.org/I4210126319",  # Duke-NUS Medical School
        "https://openalex.org/I4210097409",  # SingHealth Duke-NUS
    },
    "https://openalex.org/I241749": {  # University of Cambridge
        "https://openalex.org/I241749",
        "https://openalex.org/I4210158597",  # MRC Epidemiology Unit
        "https://openalex.org/I4210134973",  # MRC Biostatistics Unit
        "https://openalex.org/I192597271",   # MRC Cognition and Brain Sciences Unit
        "https://openalex.org/I4210159948",  # MRC Toxicology Unit
        "https://openalex.org/I4210135132",  # Cambridge University Press
        "https://openalex.org/I21196054",    # Wellcome/MRC Cambridge Stem Cell Institute
        "https://openalex.org/I4210089382",  # CRUK Cambridge Center
        "https://openalex.org/I2801782436",  # The Gurdon Institute
        "https://openalex.org/I4210116691",  # Wellcome/MRC Institute of Metabolic Science
        "https://openalex.org/I173004203",   # MRC Human Nutrition Research
        "https://openalex.org/I79266365",    # MRC Mitochondrial Biology Unit
        "https://openalex.org/I4210089716",  # MRC Cancer Unit
        "https://openalex.org/I4389425352",  # NIHR Cambridge Biomedical Research Centre
    },
    "https://openalex.org/I99065089": {  # Tsinghua University
        "https://openalex.org/I99065089",
        "https://openalex.org/I4210160507",  # Center for Life Sciences
        "https://openalex.org/I4210091786",  # State Key Lab on Integrated Optoelectronics
        "https://openalex.org/I4210114105",  # Tsinghua-Berkeley Shenzhen Institute
        "https://openalex.org/I4210159340",  # Synergetic Innovation Center for Advanced Materials
        "https://openalex.org/I4210127494",  # Collaborative Innovation Center of Quantum Matter
        "https://openalex.org/I4391767896",  # State Key Lab of New Ceramics and Fine Processing
    },
    "https://openalex.org/I49875843": {  # University of Tokyo (no children found)
        "https://openalex.org/I49875843",
    },
}


def build_tier_sets():
    """Build flat sets of all institution IDs for each tier."""
    tier1_set = set()
    for ids in TIER1_EXPANSION.values():
        tier1_set.update(ids)

    tier2_set = set(tier1_set)  # Tier 2 is inclusive of Tier 1
    for ids in TIER2_ADDITIONAL_EXPANSION.values():
        tier2_set.update(ids)
    # Also include NIH sub-institutions in DHHS (they overlap)
    return tier1_set, tier2_set


# ---------------------------------------------------------------------------
# Analysis pipeline
# ---------------------------------------------------------------------------

def load_and_analyze(analysis_dir, min_refs=10, min_papers=50):
    """Load parquet data, filter, compute per-journal rankings."""
    analysis_path = Path(analysis_dir)
    part_files = sorted(analysis_path.glob("part_*.parquet"))
    if not part_files:
        raise FileNotFoundError(f"No part files found in {analysis_path}")

    tier1_set, tier2_set = build_tier_sets()
    print(f"Tier 1: {len(tier1_set)} institution IDs")
    print(f"Tier 2: {len(tier2_set)} institution IDs (inclusive)")

    # Columns we need from the parquet files
    needed_cols = [
        "source_id", "source_name", "publisher",
        "institution_ids", "referenced_works_count",
        "topic_field_name", "topic_domain_name",
    ]

    # Per-journal accumulators
    journals = defaultdict(lambda: {
        "paper_count": 0,
        "papers_with_institutions": 0,
        "tier1_count": 0,
        "tier2_count": 0,
        "fields": Counter(),
        "domains": Counter(),
        "publishers": Counter(),
    })

    total_rows = 0
    filtered_rows = 0
    start = time.time()

    for i, pf in enumerate(part_files):
        table = pq.read_table(pf, columns=needed_cols)
        batch_size = table.num_rows
        total_rows += batch_size

        # Convert to Python for processing
        source_ids = table.column("source_id").to_pylist()
        source_names = table.column("source_name").to_pylist()
        publishers = table.column("publisher").to_pylist()
        inst_ids_col = table.column("institution_ids").to_pylist()
        ref_counts = table.column("referenced_works_count").to_pylist()
        fields = table.column("topic_field_name").to_pylist()
        domains = table.column("topic_domain_name").to_pylist()

        for j in range(batch_size):
            # Filter: minimum references
            ref_count = ref_counts[j]
            if ref_count is None or ref_count < min_refs:
                continue

            source_id = source_ids[j]
            if not source_id:
                continue

            filtered_rows += 1
            source_name = source_names[j] or "Unknown"
            key = source_id

            jdata = journals[key]
            jdata["paper_count"] += 1

            # Track publisher and field
            pub = publishers[j]
            if pub:
                jdata["publishers"][pub] += 1
            field = fields[j]
            if field:
                jdata["fields"][field] += 1
            domain = domains[j]
            if domain:
                jdata["domains"][domain] += 1

            # Store source_name (take the most common one)
            if "name" not in jdata:
                jdata["name"] = source_name

            # Institution matching
            inst_list = inst_ids_col[j]
            if inst_list and len(inst_list) > 0:
                jdata["papers_with_institutions"] += 1
                inst_set = set(inst_list)
                if inst_set & tier1_set:
                    jdata["tier1_count"] += 1
                if inst_set & tier2_set:
                    jdata["tier2_count"] += 1

        if (i + 1) % 50 == 0 or i == len(part_files) - 1:
            elapsed = time.time() - start
            print(f"  Processed {i+1}/{len(part_files)} files "
                  f"({total_rows:,} rows, {filtered_rows:,} after filter) "
                  f"[{elapsed:.1f}s]")

    print(f"\nTotal: {total_rows:,} rows, {filtered_rows:,} after ref>={min_refs} filter")
    print(f"Unique journals: {len(journals):,}")

    # Build output
    results = []
    for source_id, jdata in journals.items():
        if jdata["paper_count"] < min_papers:
            continue

        pwi = jdata["papers_with_institutions"]
        pc = jdata["paper_count"]

        # Field classification: most common field
        field = jdata["fields"].most_common(1)[0][0] if jdata["fields"] else None
        secondary_field = None
        if len(jdata["fields"]) >= 2:
            top2 = jdata["fields"].most_common(2)
            total_field_papers = sum(jdata["fields"].values())
            if top2[1][1] / total_field_papers > 0.25:
                secondary_field = top2[1][0]

        domain = jdata["domains"].most_common(1)[0][0] if jdata["domains"] else None
        publisher = jdata["publishers"].most_common(1)[0][0] if jdata["publishers"] else None

        results.append({
            "name": jdata.get("name", "Unknown"),
            "source_id": source_id,
            "publisher": publisher,
            "field": field,
            "secondary_field": secondary_field,
            "domain": domain,
            "paper_count": pc,
            "papers_with_institutions": pwi,
            "institution_coverage": round(pwi / pc, 4) if pc > 0 else 0,
            "tier1_count": jdata["tier1_count"],
            "tier1_rate": round(jdata["tier1_count"] / pwi, 6) if pwi > 0 else 0,
            "tier2_count": jdata["tier2_count"],
            "tier2_rate": round(jdata["tier2_count"] / pwi, 6) if pwi > 0 else 0,
        })

    # Sort by tier1_rate descending
    results.sort(key=lambda x: -x["tier1_rate"])

    print(f"Journals with {min_papers}+ papers: {len(results):,}")
    return results, filtered_rows


def build_metadata(total_papers, num_journals, tier1_set, tier2_set):
    """Build metadata section for the output JSON."""
    tier1_names = ["Harvard University", "Stanford University", "MIT", "University of Oxford"]
    tier2_names = tier1_names + [
        "Google", "Meta", "Microsoft", "NIH", "US DHHS",
        "Chinese Academy of Sciences", "ETH Zurich", "UCL",
        "Imperial College London", "University of Toronto",
        "UC Berkeley", "Princeton University", "Johns Hopkins University",
        "National University of Singapore", "University of Cambridge",
        "Tsinghua University", "University of Tokyo", "Max Planck Society", "CNRS",
    ]
    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "data_coverage": "~5% of OpenAlex snapshot by bytes (partial sample)",
        "date_range": "2015-2024",
        "filters": "type=article, not paratext, referenced_works_count >= 10",
        "total_papers_analyzed": total_papers,
        "total_journals": num_journals,
        "tier1_institution_ids": len(tier1_set),
        "tier2_institution_ids": len(tier2_set),
        "tier1_institutions": tier1_names,
        "tier2_institutions": tier2_names,
        "note": "Based on partial data. Rankings may shift with the full dataset.",
    }


def main():
    parser = argparse.ArgumentParser(description="Analyze OpenAlex data for journal rankings")
    parser.add_argument("--input", default=str(Path(__file__).parent / "analysis"),
                        help="Directory containing parquet part files")
    parser.add_argument("--output", default=str(Path(__file__).parent.parent / "ranking_data.json"),
                        help="Output JSON file path")
    parser.add_argument("--min-papers", type=int, default=50,
                        help="Minimum papers per journal (default: 50)")
    parser.add_argument("--min-refs", type=int, default=10,
                        help="Minimum referenced works count (default: 10)")
    args = parser.parse_args()

    print(f"Input: {args.input}")
    print(f"Output: {args.output}")
    print(f"Min papers: {args.min_papers}")
    print(f"Min references: {args.min_refs}")
    print()

    results, total_papers = load_and_analyze(
        args.input, min_refs=args.min_refs, min_papers=args.min_papers
    )

    tier1_set, tier2_set = build_tier_sets()
    metadata = build_metadata(total_papers, len(results), tier1_set, tier2_set)

    output = {"metadata": metadata, "journals": results}

    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)

    size_kb = Path(args.output).stat().st_size / 1024
    print(f"\nWrote {len(results):,} journals to {args.output} ({size_kb:.0f} KB)")

    # Print top 20
    print(f"\nTop 20 by Tier 1 rate:")
    print(f"{'Rank':>4}  {'Journal':<45} {'Papers':>7} {'Inst%':>6} {'T1%':>6} {'T2%':>6}")
    print("-" * 82)
    for i, j in enumerate(results[:20]):
        print(f"{i+1:>4}  {j['name'][:45]:<45} {j['paper_count']:>7,} "
              f"{j['institution_coverage']*100:>5.1f}% "
              f"{j['tier1_rate']*100:>5.1f}% {j['tier2_rate']*100:>5.1f}%")


if __name__ == "__main__":
    main()
