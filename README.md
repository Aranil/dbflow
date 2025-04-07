# dbflow
Module to handle SQLite database operations and provide utilities

To install the module run

pip install dbflow@git+https://github.com/Aranil/dbflow.git@main to install package


To install required packages in conda env

run conda env create -f environment.yml 



## Customizable SQL Folder and `db_structure.py`

The `dbflow` module provides flexibility for customization through a `custom_template` folder, which contains example SQL scripts and table definitions. Users can extend or modify these examples to suit their specific requirements.

### Key Features
- **Custom SQL Folder**: A folder where users can add their own `.sql` files for custom database operations.
- **Execution History**: All executed or rendered SQL commands are stored in a dedicated folder named `_sql_executed`, located outside the module directory, for tracking and debugging.
- **Dynamic Inputs**: Create reusable, parameterized SQL scripts that accept dynamic inputs for flexible database queries.
- **`db_structure.py` Script**: A Python script that allows users to define or modify table structures programmatically.

---

### How It Works

#### **1. Place Custom SQL Scripts**
Place your custom SQL scripts in the `dbflow/sql` folder (or a copied version of the `custom_template/sql` folder in your project). SQL scripts can be:
- **Parameterized**: Use placeholders like `:param_name` for dynamic inputs.
- **Static**: Define fixed, predefined SQL operations.

When a script is executed, the rendered SQL (with parameters applied, if any) is stored in the `_sql_executed` folder for auditing or debugging purposes.

```python
import dbflow.src.db_utility as db_utility

# Render and execute a parameterized SQL script
sql = db_utility.create_sql(
    sql_file='custom_query.sql',
    replacements={
        ':formatted_date': formatted_date,
        ':sl_nr': sl_nr,
        ':CROP_TYPE_CODE': CROP_TYPE_CODE,
    }
)
db_utility.query_sql(sql=sql, db_engine=database.archive.engine)
```


#### **Application Directory (`my_application/`)**
```plaintext
my_application/
├── custom/                 # User-specific customizations
│   ├── db_structure.py     # User-defined database schema
│   ├── sql/                # User-defined SQL scripts
│   │   ├── custom_query.sql
│   │   ├── example_schema.sql
├── main.py                 # Application entry point


```
#### **Application Directory (`dbflow/`)**
```plaintext
├── dbflow/                 # Installed dbflow module
│   ├── src/                # Core logic of dbflow
│   │   ├── db_sqlalchemy.py  # Core SQLAlchemy-related utilities
│   │   ├── db_utility.py     # Utility functions (e.g., `get_custom_paths`, `load_custom_structure`)
│   ├── additional/         # Additional utilities and scripts
│   │   ├── _create_db_report.py  # Generate database schema reports
│   │   ├── _compare_tables_from_db.py  # Compare table data between databases
│   ├── config.ini          # Configuration file for paths
├── custom_template/        # Example user-specific customization (outside `dbflow`)
│   ├── db_structure.py
│   ├── sql/
│   │   ├── example_query.sql

```

2. Use `db_structure.py`  script to programmatically define or modify database table structures. This script can be copied from the `custom_template` folder into your project and extended as needed.
3. Execute your custom SQL or table structure changes through the `dbflow` module.



### Update `config.ini`

Edit `config.ini` in the `dbflow` module to point to the copied `custom` folder in your project:

Paths should be defined relative to the location of this config.ini file.
Use forward slashes (`/`) for cross-platform compatibility.

```ini
[paths]
custom_sql_dir = ./custom/sql
custom_db_structure = ./custom/db_structure.py
executed_sql_dir = ../_sql_executed
```

Logging Configuration. To manage logging settings, you can adjust the log level and change the log file location as needed.
```ini
[logging]
level = WARNING                               # Change to INFO, DEBUG, ERROR, or CRITICAL as needed
format = %(asctime)s - %(name)s - %(levelname)s - %(message)s
log_file = ../_logs/module.log                # Change the filename or directory if needed
```

### Default Behavior

If `config.ini` is not updated, `dbflow` will fall back to using the `custom_template` folder included in the module.


### Example: Using a Custom `db_structure.py`

The following example demonstrates how to use the `load_custom_structure` function to dynamically load a custom `db_structure.py` file based on the paths configured in `config.ini`. If the custom structure is found, it initializes the database using the user-defined table schema. If not, it falls back to the default behavior.

#### Code Example
```python
from dbflow.src.db_utility import load_custom_structure
from dbflow.src.db_utility import connect2db
from decouple import config

def main():
    # Connect to the database
    dbarchive = connect2db(config('DB_PATH_MAIN'))
    engine = dbarchive.archive.engine

    # Load the custom db_structure.py
    custom_structure = load_custom_structure()

    if custom_structure:
        # If the custom structure is found, initialize the database
        with engine.connect() as connection:
            cursor = connection.connection.cursor()
            custom_structure.define_tables(cursor)
            connection.commit()
        print("Custom tables initialized successfully!")
    else:
        print("No custom structure found. Using default behavior.")

if __name__ == "__main__":
    main()
```    
#### Code Example to create table and insert data
```python
from dbflow.src.db_utility import connect2db, create_sql

dbarchive = connect2db(r'...db\RCM.db')
engine = dbarchive.archive.engine
sql = create_sql(r"...sql\ml_transferability.sql")

table_name = 'ml_transferability'
dbarchive.create_table_from_sql(sql, table_name)
          
# df is pandas dataframe
pim_key = dbarchive.get_primary_keys(table_name)
dbarchive.insert(table=table_name , primary_key=pim_key, orderly_data=df.to_dict(orient='records'), update=True)
``` 




# Additional Scripts

The `additional/` folder contains utility scripts to extend the functionality of the application. These scripts are designed for specific tasks such as visualizing database relationships, comparing tables across databases, or generating reports.

## Scripts in this Folder

### 1. `_create_db_reports.py`

#### Purpose
Generates a detailed report of the database schema and plots a connection graph of the tables. It visualizes relationships between tables and highlights primary and foreign key dependencies.

#### Features
- Reads the database schema.
- Visualizes table relationships using a directed graph.
- Identifies primary keys and foreign key dependencies.

#### Key Functions
- **`get_table_relationships(db_path, conn, excluded_prefixes)`**
  - Extracts table relationships and primary key information.
  - **Parameters:**
    - `db_path`: Path to the SQLite database file.
    - `conn`: SQLite database connection.
    - `excluded_prefixes`: List of table prefixes to exclude from processing.
  - **Returns:** A DataFrame of relationships and a dictionary of primary keys.

- **`plot_relationship_graph(relationships_df, primary_keys, group_colors, ...)`**
  - Plots a graph of table relationships.
  - **Parameters:**
    - `relationships_df`: DataFrame of table relationships.
    - `primary_keys`: Dictionary of primary keys for each table.
    - `group_colors`: Color groups for tables in the graph.
  - **Returns:** None (Displays or saves the graph).

#### Example Usage
```python
from additional._create_db_reports import get_table_relationships, plot_relationship_graph

db_path = r'...\RCM.db'
dbarchive = connect2db(db_path)  # Connect to your database
conn = dbarchive.archive.conn

# Get table relationships
relationships, primary_keys = get_table_relationships(conn=conn, excluded_prefixes=['temp'])

# Plot the relationships
plot_relationship_graph(relationships, primary_keys, group_colors={"users": "blue", "orders": "green"})
```

Contributors: [Markus Zehner](https://github.com/MarkusZehner/)


## Citation
If you use this project in your work, please cite it using the information in the [CITATION.cff](./CITATION.cff) file or via the "Cite this repository" button on GitHub.

Thank you for supporting open science! 🙌