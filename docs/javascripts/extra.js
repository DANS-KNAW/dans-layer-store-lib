function setColumnWidths(firstColumnCaption, ...widths) {
    const tables = document.querySelectorAll('table');
    tables.forEach(table => {
        const firstHeader = table.querySelector('th');
        if (firstHeader && firstHeader.textContent.trim() === firstColumnCaption) {
            const cols = table.querySelectorAll('th, td');
            widths.forEach((width, index) => {
                const cells = table.querySelectorAll(`tr th:nth-child(${index + 1}), tr td:nth-child(${index + 1})`);
                cells.forEach(cell => {
                    cell.style.width = width;
                });
            });
        }
    });
}
