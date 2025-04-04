# Snowflake Data Scan

This script connects to a Snowflake database, retrieves data from specified tables, submits it for data scanning, tracks the scan status, and saves the results to an Excel file.
Explore Protecto data scanning API documentation in [https://docs.protecto.ai/docs/data-scan-async/](https://docs.protecto.ai/docs/data-scan-async/).

## Prerequisites

- Python 3.10 +
- Required Python libraries: `requests`, `sqlalchemy`, `openpyxl`, `snowflake.sqlalchemy`
- Credentials stored in a JSON file
- Protecto API key for data scanning

## Installation

1. Install the required dependencies using pip:

   ```sh
   pip install requests sqlalchemy openpyxl snowflake-sqlalchemy
   ```

## Configuration

### 1. Credentials File

Store your Snowflake credentials and Protecto API key in a JSON file (e.g., `credentials.json`) with the following format:

```json
{
    "account": "your_snowflake_account",
    "user": "your_snowflake_username",
    "password": "your_snowflake_password",
    "warehouse": "your_snowflake_warehouse",
    "role": "your_snowflake_role",
    "protecto_api_key": "your_protecto_api_key"
}
```

To get the Protecto API key, please contact [help@protecto.ai](mailto:help@protecto.ai).

### 2. Table List File

Please enter the list of tables to be processed, with one table per line in `tables.txt` 

```
database_name.schema_name.table_name
database_name.schema_name.table_name_2
```

## Usage

Run the script with the following command:

```sh
python3 sf_data_scan.py
```

## How It Works

1. Loads credentials from `credentials.json`, including Snowflake credentials and the Protecto API key.
2. Reads table names from `tables.txt`.
3. Connects to Snowflake and fetches a limited number of rows from each table.
4. Splits data into chunks of 5 columns each and submits it to the API for scanning.
5. Tracks the scan status until completion.
6. Retrieves the scan report and saves it to `data_scan_report.xlsx`.

## Output

- The scan results are saved in `data_scan_report.xlsx`.
- Each row contains details of columns analyzed, identified values, and classification.
- The Excel file will have merged cells for `column_name` where multiple results exist.

## Handling Paginated API Responses

If the API response includes a `next_page_token`, the script will make additional requests until all data is retrieved. The results are appended together to ensure completeness.

## Error Handling

- If an API request fails, the script raises an exception with the error message.
- If Snowflake credentials are incorrect, authentication will fail.
- The script handles cases where tables do not exist or lack sufficient data.

## Customization

- Modify `NUM_ROWS` to change the number of rows fetched per table.

```python
NUM_ROWS = 100  # Adjust as needed
```

- Adjust the `chunk_size` in `split_columns()` to control column chunking.

```python
def split_columns(columns, rows, chunk_size=5):
```

## License

This script is open for customization and use as per your project needs.

