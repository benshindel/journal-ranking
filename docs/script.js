document.addEventListener("DOMContentLoaded", function () {
    fetch("ranking_data.json")
        .then((r) => r.json())
        .then((data) => initApp(data))
        .catch((err) => {
            document.getElementById("ranking-table").textContent =
                "Error loading data: " + err.message;
        });
});

function initApp(data) {
    const journals = data.journals;
    const metadata = data.metadata;

    // Populate filter dropdowns
    const fields = [...new Set(journals.map((j) => j.field).filter(Boolean))].sort();
    const domains = [...new Set(journals.map((j) => j.domain).filter(Boolean))].sort();

    const fieldSelect = document.getElementById("field-filter");
    fields.forEach((f) => {
        const opt = document.createElement("option");
        opt.value = f;
        opt.textContent = f;
        fieldSelect.appendChild(opt);
    });

    const domainSelect = document.getElementById("domain-filter");
    domains.forEach((d) => {
        const opt = document.createElement("option");
        opt.value = d;
        opt.textContent = d;
        domainSelect.appendChild(opt);
    });

    // Rate formatter: color-coded percentage
    function rateFormatter(cell) {
        const val = cell.getValue();
        if (val == null) return "";
        const pct = (val * 100).toFixed(1) + "%";
        if (val >= 0.1) return '<span class="rate-high">' + pct + "</span>";
        if (val >= 0.03) return '<span class="rate-medium">' + pct + "</span>";
        return '<span class="rate-low">' + pct + "</span>";
    }

    function pctFormatter(cell) {
        const val = cell.getValue();
        if (val == null) return "";
        return (val * 100).toFixed(0) + "%";
    }

    function numberFormatter(cell) {
        const val = cell.getValue();
        if (val == null) return "";
        return val.toLocaleString();
    }

    // Build table
    const table = new Tabulator("#ranking-table", {
        data: journals,
        layout: "fitColumns",
        pagination: true,
        paginationSize: 50,
        paginationSizeSelector: [25, 50, 100, 200],
        initialSort: [{ column: "tier1_rate", dir: "desc" }],
        columns: [
            {
                title: "#",
                formatter: "rownum",
                width: 45,
                hozAlign: "center",
                headerSort: false,
                frozen: true,
                widthShrink: 0,
            },
            {
                title: "Journal",
                field: "name",
                widthGrow: 3,
                minWidth: 150,
                frozen: true,
                headerFilter: "input",
                headerFilterPlaceholder: "Search...",
            },
            {
                title: "Publisher",
                field: "publisher",
                widthGrow: 2,
                minWidth: 100,
                headerFilter: "input",
                headerFilterPlaceholder: "Search...",
            },
            {
                title: "Field",
                field: "field",
                widthGrow: 1.5,
                minWidth: 100,
            },
            {
                title: "Domain",
                field: "domain",
                width: 140,
                visible: false,
            },
            {
                title: "Papers",
                field: "paper_count",
                width: 95,
                hozAlign: "right",
                sorter: "number",
                formatter: numberFormatter,
                widthShrink: 0,
            },
            {
                title: "Cov.",
                field: "institution_coverage",
                width: 80,
                hozAlign: "right",
                sorter: "number",
                formatter: pctFormatter,
                tooltip: "Institution Coverage — % of papers with at least one identified institution",
                widthShrink: 0,
            },
            {
                title: "Tier 1",
                field: "tier1_rate",
                width: 90,
                hozAlign: "right",
                sorter: "number",
                formatter: rateFormatter,
                tooltip: "% of papers with a Tier 1 institution author",
                widthShrink: 0,
            },
            {
                title: "Tier 2",
                field: "tier2_rate",
                width: 90,
                hozAlign: "right",
                sorter: "number",
                formatter: rateFormatter,
                tooltip: "% of papers with a Tier 1 or Tier 2 institution author",
                widthShrink: 0,
            },
        ],
    });

    // External filters
    function applyFilters() {
        const fieldVal = fieldSelect.value;
        const domainVal = domainSelect.value;
        const minPapers = parseInt(document.getElementById("min-papers").value);
        const minCoverage = parseFloat(document.getElementById("min-coverage").value);

        const filters = [];
        if (fieldVal) filters.push({ field: "field", type: "=", value: fieldVal });
        if (domainVal) filters.push({ field: "domain", type: "=", value: domainVal });
        if (minPapers > 0)
            filters.push({ field: "paper_count", type: ">=", value: minPapers });
        if (minCoverage > 0)
            filters.push({
                field: "institution_coverage",
                type: ">=",
                value: minCoverage,
            });

        table.setFilter(filters);
    }

    function updateCount() {
        const count = table.getDataCount("active");
        document.getElementById("result-count").textContent =
            count.toLocaleString() + " journals shown";
    }

    fieldSelect.addEventListener("change", () => {
        applyFilters();
        updateCount();
    });
    domainSelect.addEventListener("change", () => {
        applyFilters();
        updateCount();
    });
    document.getElementById("min-papers").addEventListener("change", () => {
        applyFilters();
        updateCount();
    });
    document.getElementById("min-coverage").addEventListener("change", () => {
        applyFilters();
        updateCount();
    });

    // Apply initial filters and count
    table.on("tableBuilt", () => {
        applyFilters();
        updateCount();
    });
    table.on("dataFiltered", () => {
        updateCount();
    });
}
