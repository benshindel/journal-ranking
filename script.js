document.addEventListener("DOMContentLoaded", function() {
    fetch('ranking_data.csv') // Fetch your CSV data
        .then(response => response.text())
        .then(csvData => {
            const tableData = Papa.parse(csvData, { header: true, dynamicTyping: true }).data; // Parse CSV, header row, type conversion

            // Initialize Tabulator
            const table = new Tabulator("#ranking-table", {
                data: tableData,
                autoColumns: true, // Generate columns from header row
                sortable: true,
                pagination: "local", // Enable local pagination for large datasets
                paginationSize: 25, // Number of rows per page
            });

            // --- Create Dropdown Filters ---
            const filtersSection = document.getElementById('filters-section');
            const filterableColumns = ['Publisher']; // Columns you want to filter by

            filterableColumns.forEach(columnName => {
                const selectElement = document.createElement('select');
                selectElement.classList.add('filter-dropdown'); // For styling if needed
                const defaultOption = document.createElement('option');
                defaultOption.value = '';
                defaultOption.text = `Filter by ${columnName}`;
                selectElement.appendChild(defaultOption);

                // Get unique values for the dropdown options
                const uniqueValues = [...new Set(tableData.map(row => row[columnName]))].filter(Boolean).sort(); // Get unique values, remove empty, sort

                uniqueValues.forEach(value => {
                    const option = document.createElement('option');
                    option.value = value;
                    option.text = value;
                    selectElement.appendChild(option);
                });

                selectElement.addEventListener('change', function() {
                    const selectedValue = this.value;
                    table.setFilter(columnName, "=", selectedValue); // Tabulator's filter function
                });

                filtersSection.appendChild(selectElement);
            });
        });
});
