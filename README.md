# dbflow
Module to handle SQLite database operations and provide utilities



## Customizable SQL Folder and `db_structure.py`

The `dbflow` module provides a flexible structure for managing a database schema and queries:
- **Custom SQL Folder**: A folder where one can add its own `.sql` files for custom database operations. 
- **Execution History**: All executed or rendered SQL commands are stored in `_sql_executed` for tracking.
- **Dynamic Inputs**: Create reusable parameterized scripts for flexible database queries.
- **`db_structure.py` Script**: A Python script that allows one to define or modify table structures programmatically.

### How It Works
1. Place your custom SQL scripts in the `dbflow/sql` folder. These scripts can be parameterized for dynamic input or static for predefined operations.
  When a script is executed, the SQL command (rendered with parameters, if applicable) is stored in a dedicated folder named _sql_executed outside the module directory.
  This ensures a record of all executed commands for auditing or debugging.
  Use placeholders like :param_name to create reusable SQL scripts that accept dynamic inputs.
  Example of execution with parameters:
   
    import db_utility
   
    sql = db_utility.create_sql(sql_file='_custom_query.sql', 
                    replacements={':formatted_date': formatted_date,
                                  ':sl_nr': sl_nr,
                                  ':CROP_TYPE_CODE': CROP_TYPE_CODE
                                    }
                    )
    db_utility.query_sql(sql='custom_query.sql', db_engine=dbatabase.archive.engine)


2. Use `db_structure.py` to programmatically define or modify database table structures.
3. Execute your custom SQL or table structure changes through the `dbflow` module.



To install the module run

run pip install git+ssh://git@github.com/Aranil/dbflow.git to install package

To install required packages in conda env

run conda env create -f environment.yml 


customized .sql files in folder sql - contact @Aranil to add to your module


