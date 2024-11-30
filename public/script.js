function createTable(data) {
    if (!data || data.length === 0) {
        return '<p>No data found</p>';
    }

    // Create a table element
    let table = document.createElement('table');
    table.style.width = '100%';
    table.setAttribute('border', '1');
    table.style.borderCollapse = 'collapse';
    table.style.marginTop = '20px';

    // Create the table header
    let thead = document.createElement('thead');
    let headerRow = document.createElement('tr');

    // Populate header row with column names
    Object.keys(data[0]).forEach(key => {
        let th = document.createElement('th');
        th.textContent = key;
        th.style.backgroundColor = '#005f73';
        th.style.color = 'white';
        th.style.padding = '8px';
        th.style.textAlign = 'center';
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    // Create the table body
    let tbody = document.createElement('tbody');

    data.forEach(row => {
        let tr = document.createElement('tr');
        Object.values(row).forEach(value => {
            let td = document.createElement('td');
            td.textContent = value;
            td.style.padding = '8px';
            td.style.textAlign = 'center';
            if (value instanceof Date) {
                td.textContent = value.toLocaleString(); // Format date nicely if needed
            }
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    return table.outerHTML;
}

// Function to handle form submission for Fetch data
document.getElementById('fetchForm').onsubmit = async function (e) {
    e.preventDefault();
    const tableName = document.getElementById('fetchTableName').value;
    const response = await fetch(`http://localhost:5001/fetch?table_name=${tableName}`);
    const data = await response.json();

    // Display the data in the result box as a table
    document.getElementById('fetchResult').innerHTML = createTable(data);
};

// Function to handle form submission for Search data
document.getElementById('searchForm').onsubmit = async function (e) {
    e.preventDefault();
    const searchQuery = document.getElementById('searchQuery').value;
    const locationId = document.getElementById('searchLocation').value;
    const category = document.getElementById('searchCategory').value;
    const minTickets = document.getElementById('minTickets').value;
    const maxTickets = document.getElementById('maxTickets').value;
    const dateTime = document.getElementById('dateTime').value;

    // Construct the request body
    const requestBody = {
        search_query: searchQuery,
        location_id: locationId,
        category: category,
        min_tickets: minTickets,
        max_tickets: maxTickets,
        date_time: dateTime
    };

    try {
        // Send request to the server for searching events
        const response = await fetch('http://localhost:5001/search', {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody)
        });

        const data = await response.json();
        document.getElementById('searchResult').innerText = JSON.stringify(data, null, 2);
    } catch (error) {
        console.error('Error fetching search results:', error);
        document.getElementById('searchResult').innerText = 'An error occurred while searching.';
    }
};


// Function to handle form submission for Insert data
document.getElementById('insertForm').onsubmit = async function (e) {
    e.preventDefault();
    const tableName = document.getElementById('insertTable').value;
    const columns = document.getElementById('insertColumns').value.split(',').map(col => col.trim());
    const values = document.getElementById('insertValues').value.split(',').map(val => val.trim());

    const requestBody = {
        table_name: tableName,
        columns: columns,
        values: values
    };

    const response = await fetch('http://localhost:5001/insert', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody)
    });

    const data = await response.json();
    document.getElementById('insertResult').innerText = JSON.stringify(data, null, 2);
};

// Function to handle form submission for Update data
document.getElementById('updateForm').onsubmit = async function (e) {
    e.preventDefault();
    const tableName = document.getElementById('updateTable').value;
    const columns = document.getElementById('updateColumns').value.split(',').map(col => col.trim());
    const values = document.getElementById('updateValues').value.split(',').map(val => val.trim());
    const primaryColumn = document.getElementById('updatePrimaryColumn').value;
    const primaryValue = document.getElementById('updatePrimaryValue').value;

    const requestBody = {
        table_name: tableName,
        columns: columns,
        values: values,
        primary_column: primaryColumn,
        primary_value: primaryValue
    };

    const response = await fetch('http://localhost:5001/update', {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody)
    });

    const data = await response.json();
    document.getElementById('updateResult').innerText = JSON.stringify(data, null, 2);
};

// Function to handle form submission for Delete data
document.getElementById('deleteForm').onsubmit = async function (e) {
    e.preventDefault();
    const tableName = document.getElementById('deleteTable').value;
    const primaryColumn = document.getElementById('deletePrimaryColumn').value;
    const primaryValue = document.getElementById('deletePrimaryValue').value;

    const requestBody = {
        table_name: tableName,
        primary_column: primaryColumn,
        primary_value: primaryValue
    };

    const response = await fetch('http://localhost:5001/delete', {
        method: 'DELETE',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody)
    });

    const data = await response.json();
    document.getElementById('deleteResult').innerText = JSON.stringify(data, null, 2);
};
