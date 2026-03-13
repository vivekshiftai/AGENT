# Column metadata for analytical column selection

Place Excel or CSV files here to improve LLM column selection. The column selection node uses **Description** and **Short Text** from these files as labels and context for each column, so the LLM can select columns more accurately.

## File format

Your file must have these headers (exact names or case-insensitive match):

| Header         | Description |
|----------------|-------------|
| **Value Field** | Technical column name (must match the column name from the data source / SAP view). |
| **Description** | Full description of the column (used as context for the LLM). |
| **Short Text**  | Short, user-friendly label (used as the display label in prompts). |

- **Excel**: `.xlsx` — the first sheet is read.
- **CSV**: `.csv` — first row must be the header.

## Behaviour

- The loader looks for any `.xlsx` or `.csv` file in this folder (first file found is used).
- Rows are mapped by **Value Field** (trimmed). **Description** and **Short Text** are stored and applied to matching columns when building the column list for the LLM.
- **Matching is case-insensitive**: a column in the schema (e.g. `NET_VALUE`) will match a row in the file whose Value Field is `Net_Value` or `net_value`. The Excel **Description** and **Short Text** are then used as that column’s description and label in the “available columns” shown to the LLM.
- If a column from the schema is not in the file, the existing label from the schema is kept.

## Example

| Value Field   | Description                          | Short Text    |
|---------------|--------------------------------------|---------------|
| Net_Value     | Total net sales value in document currency | Net Sales  |
| Order_Count   | Number of sales orders               | Order Count   |
| Plant         | Plant / manufacturing location      | Plant         |

After you add a file here, the column selection prompt will show the LLM lines like:

`Net_Value | Net Sales | dimension — Total net sales value in document currency`

So the LLM sees the **Short Text** as the label and the **Description** in the available columns, and can make better choices. Matching to schema columns is **case-insensitive** (e.g. `NET_VALUE` in the view will use the row where Value Field is `Net_Value`).
