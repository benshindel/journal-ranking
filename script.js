document.addEventListener("DOMContentLoaded", function() {
    fetch('ranking_data.csv')
        .then(response => response.text())
        .then(csvData => {
            const tableData = Papa.parse(csvData, { header: true, dynamicTyping: true }).data;

            const table = new Tabulator("#ranking-table", {
                data: tableData,
                autoColumns: true,
                sortable: true,
                pagination: "local",
                paginationSize: 25,
            });

            const filtersSection = document.getElementById('filters-section');
            const filterableColumns = ['Publisher'];

            filterableColumns.forEach(columnName => {
                const filterContainer = document.createElement('div');
                filterContainer.classList.add('filter-container');

                const label = document.createElement('label');
                label.textContent = `Filter by ${columnName}:`;
                filterContainer.appendChild(label);

                const selectElement = document.createElement('select');
                selectElement.multiple = true;
                filterContainer.appendChild(selectElement);

                // Add "Select All" Option
                const defaultOption = document.createElement('option');
                defaultOption.value = ''; // Empty value for clearing
                defaultOption.text = `Select ${columnName}`;
                selectElement.appendChild(defaultOption);

                const uniqueValues = [...new Set(tableData.map(row => row[columnName]))].filter(Boolean).sort();

                uniqueValues.forEach(value => {
                    const option = document.createElement('option');
                    option.value = value;
                    option.text = value;
                    selectElement.appendChild(option);
                });

                selectElement.addEventListener('change', function() {
                    const selectedValues = Array.from(this.selectedOptions).map(option => option.value).filter(value => value !== ''); // Filter out empty value

                    if (selectedValues.length > 0) {
                        table.setFilter(function(row) {
                            return selectedValues.includes(row[columnName]);
                        });
                    } else {
                        table.clearFilter(columnName);
                    }
                });

                filtersSection.appendChild(filterContainer);
            });
        });
});
