'''
module to extract the DB content info into csv file
compare DB content and return the differences in the row (column - not integrated yet) wwrite the result to a log file
'''


from db_utility import connect2db
import pandas as pd
import logging


DB_PATH1 = r'..\RCM_broken.db'
DB_PATH2 = r'..\RCM.db'




# Set up logging
logging.basicConfig(filename='../../../UAVpExtraction/src/db/db_comparison.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

#print(dbarchive1.get_primary_keys('uavsemivarmodelperform'))
#print(dbarchive1.get_colnames('uavsemivarmodelperform'))


def compare_dataframes(table_name, df1, df2, db_path1, db_path2, primary_keys=None):
    # Align DataFrames by their columns
    df1, df2 = df1.align(df2, join='outer', axis=1, fill_value=None)

    # Compare DataFrames
    are_equal = df1.equals(df2)
    logging.info(f"Are the DataFrames equal for table {table_name}? {are_equal}")
    print(f"Are the DataFrames equal for table {table_name}? {are_equal}")

    if not are_equal:
        # Find rows in df1 that are not in df2
        diff_df1 = pd.concat([df1, df2, df2]).drop_duplicates(keep=False)

        # Find rows in df2 that are not in df1
        diff_df2 = pd.concat([df2, df1, df1]).drop_duplicates(keep=False)

        if not diff_df1.empty:
            # Extract primary keys or row index for diff_df1
            if primary_keys:
                key_info_df1 = diff_df1[primary_keys].drop_duplicates()
            else:
                key_info_df1 = diff_df1.index

            logging.info(f"Rows in {table_name} ({db_path1}) but not in {db_path2}: Primary Keys/Indices:\n{key_info_df1}")
            print(f"Rows in {table_name} ({db_path1}) but not in {db_path2}: Primary Keys/Indices:\n{key_info_df1}")

        if not diff_df2.empty:
            # Extract primary keys or row index for diff_df2
            if primary_keys:
                key_info_df2 = diff_df2[primary_keys].drop_duplicates()
            else:
                key_info_df2 = diff_df2.index

            logging.info(f"Rows in {table_name} ({db_path2}) but not in {db_path1}: Primary Keys/Indices:\n{key_info_df2}")
            print(f"Rows in {table_name} ({db_path2}) but not in {db_path1}: Primary Keys/Indices:\n{key_info_df2}")

    else:
        logging.info(f"No differences found in table {table_name}.")
        print(f"No differences found in table {table_name}.")



# Function to store information about tables in DB1 into a CSV
def store_db_info_csv(table_names, db_path, output_csv):
    # Initialize an empty DataFrame to store the table information for dbarchive1
    table_info_df = pd.DataFrame(columns=['Table Name', 'Exists in DB', 'Is Empty',
                                          'Has Geometry Column', 'Primary Keys', 'Column Names'])

    # Iterate through each table, filtering out tables based on prefixes
    for table_name in table_names:
        if not (table_name.startswith('geometry') or
                table_name.startswith('idx') or
                table_name.startswith('virts') or
                table_name.startswith('views')):

            print(f"Processing table: {table_name}")

            # Initialize variables for this table
            exists_in_db1 = False
            is_empty = None
            has_geometry_col = None
            primary_keys = None
            column_names = None

            # Check if the table exists in dbarchive1
            dbarchive = connect2db(db_path)  # Connect to your database
            if dbarchive.check_table_exists(table_name):
                exists_in_db1 = True
                df = dbarchive.fetch_records(table=table_name, selected_columns='*')
                df = pd.DataFrame(df)
                column_names = dbarchive.get_colnames(table_name)  # Retrieve column names
                is_empty = df.empty
                geomcolname_list = dbarchive.get_geom_colnames(table_name)
                has_geometry_col = bool(geomcolname_list)  # Check if geometry column exists
                primary_keys = dbarchive.get_primary_keys(table_name)  # Retrieve primary keys
            else:
                logging.warning(f"Table {table_name} does not exist in {db_path}.")
                print(f"Table {table_name} does not exist in {db_path}.")
                df = None

            # Store information in the table_info_df DataFrame
            table_info_df = table_info_df.append({
                'Table Name': table_name,
                'Exists in DB': exists_in_db1,
                'Is Empty': is_empty,
                'Has Geometry Column': has_geometry_col,
                'Primary Keys': primary_keys,
                'Column Names': column_names
            }, ignore_index=True)

    # Save the collected table information to a CSV file
    table_info_df.to_csv(output_csv, index=False)
    print(f"Table information for DB saved to {output_csv}")
    logging.info(f"Table information for DB saved to {output_csv}")


# Main script
def main():

    '''
    # List of table names to check
    dbarchive2 = connect2db(DB_PATH2)
    table_names = dbarchive2.archive.get_tablenames(return_all=True)  #


    # Run the store_db1_info_csv function to store information about dbarchive1
    store_db_info_csv(table_names, DB_PATH2, 'db_table_info.csv')

    # Additional logic (e.g., DataFrame comparisons) can be added here if needed
    logging.info("Table information collection completed for DB1.")
    print("Table information collection completed for DB1.")
    '''



    # Connect to both databases
    dbarchive1 = connect2db(DB_PATH1)  # Connect to DB1
    dbarchive2 = connect2db(DB_PATH2)  # Connect to DB2

    table_names = dbarchive2.archive.get_tablenames()

    for i, table_name in enumerate(table_names):
        if not (table_name.startswith('geometry') or table_name.startswith('idx') or table_name.startswith('virts') or table_name.startswith('views')):
            print(table_name)

        # Check if the table exists in both databases
        if dbarchive1.check_table_exists(table_name) and dbarchive2.check_table_exists(table_name):
            # Retrieve DataFrames from both databases
            df1 = pd.DataFrame(dbarchive1.fetch_records(table=table_name, selected_columns='*'))
            df2 = pd.DataFrame(dbarchive2.fetch_records(table=table_name, selected_columns='*'))

            # Apply the compare_dataframes function
            compare_dataframes(table_name, df1, df2, DB_PATH1, DB_PATH2)

        else:
            logging.info(f"Table {table_name} does not exist in one or both databases.")
            print(f"Table {table_name} does not exist in one or both databases.")


if __name__ == "__main__":
    main()






