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
                const filterContainer = document.createElement('div'); // Container for each filter set
                filterContainer.classList.add('filter-container');

                const label = document.createElement('label');
                label.textContent = `Filter by ${columnName}:`;
                filterContainer.appendChild(label);

                const selectElement = document.createElement('select');
                selectElement.classList.add('filter-dropdown');
                selectElement.multiple = true; // Enable multiple selection

                const defaultOption = document.createElement('option');
                defaultOption.value = '';
                defaultOption.text = `Select ${columnName}(s)`;
                selectElement.appendChild(defaultOption);

                const uniqueValues = [...new Set(tableData.map(row => row[columnName]))].filter(Boolean).sort();

                uniqueValues.forEach(value => {
                    const option = document.createElement('option');
                    option.value = value;
                    option.text = value;
                    selectElement.appendChild(option);
                });

                filterContainer.appendChild(selectElement);

                const clearButton = document.createElement('button');
                clearButton.textContent = 'Clear Filters';
                clearButton.addEventListener('click', function() {
                    selectElement.value = ''; // Clear selection
                    table.clearFilter(); // Clear Tabulator filters
                });
                filterContainer.appendChild(clearButton);

                selectElement.addEventListener('change', function() {
                    const selectedValues = Array.from(this.selectedOptions).map(option => option.value).filter(value => value !== ''); // Get selected values, exclude empty

                    if (selectedValues.length > 0) {
                        table.setFilter(function(row) {
                            return selectedValues.includes(row[columnName]);
                        });
                    } else {
                        table.clearFilter(columnName); // Clear filter if no selection
                    }
                });

                filtersSection.appendChild(filterContainer);
            });
        });
});
