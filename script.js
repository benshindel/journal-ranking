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

                const dropdownContainer = document.createElement('div');
                dropdownContainer.classList.add('dropdown-container');

                const dropdownButton = document.createElement('button');
                dropdownButton.textContent = `Select ${columnName}(s)`;
                dropdownButton.classList.add('dropdown-button');
                dropdownContainer.appendChild(dropdownButton);

                const dropdownContent = document.createElement('div');
                dropdownContent.classList.add('dropdown-content');
                dropdownContainer.appendChild(dropdownContent);

                const uniqueValues = [...new Set(tableData.map(row => row[columnName]))].filter(Boolean).sort();

                uniqueValues.forEach(value => {
                    const checkboxContainer = document.createElement('div');
                    checkboxContainer.classList.add('checkbox-container');

                    const checkbox = document.createElement('input');
                    checkbox.type = 'checkbox';
                    checkbox.value = value;
                    checkbox.id = `checkbox-${columnName}-${value}`; // Unique ID

                    const checkboxLabel = document.createElement('label');
                    checkboxLabel.textContent = value;
                    checkboxLabel.htmlFor = `checkbox-${columnName}-${value}`; // Connect label to checkbox

                    checkboxContainer.appendChild(checkbox);
                    checkboxContainer.appendChild(checkboxLabel);
                    dropdownContent.appendChild(checkboxContainer);
                });

                dropdownButton.addEventListener('click', function() {
                    dropdownContent.classList.toggle('show');
                });

                const clearButton = document.createElement('button');
                clearButton.textContent = 'Clear Filters';
                clearButton.addEventListener('click', function() {
                    const checkboxes = dropdownContent.querySelectorAll('input[type="checkbox"]');
                    checkboxes.forEach(checkbox => checkbox.checked = false);
                    table.clearFilter();
                });
                filterContainer.appendChild(clearButton);

                dropdownContent.addEventListener('click', function(event) {
                    if (event.target.type === 'checkbox') {
                        const selectedValues = Array.from(dropdownContent.querySelectorAll('input[type="checkbox"]:checked')).map(checkbox => checkbox.value);

                        if (selectedValues.length > 0) {
                            table.setFilter(function(row) {
                                return selectedValues.includes(row[columnName]);
                            });
                        } else {
                            table.clearFilter(columnName);
                        }
                    }
                });

                filtersSection.appendChild(filterContainer);
            });

            window.addEventListener('click', function(event) {
                if (!event.target.matches('.dropdown-button')) {
                    const dropdowns = document.querySelectorAll('.dropdown-content');
                    dropdowns.forEach(dropdown => {
                        if (dropdown.classList.contains('show')) {
                            dropdown.classList.remove('show');
                        }
                    });
                }
            });
        });
});
