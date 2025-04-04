# -*- coding: utf-8 -*-
"""
Setup for a database

@author: Aranil
"""
import importlib
import inspect
import sqlite3
import pkg_resources
import pandas as pd
import geopandas as gpd
from shapely.geometry import box

from sqlalchemy import create_engine, MetaData
from sqlalchemy import exists
from sqlalchemy.dialects import sqlite
from sqlalchemy.sql import text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table
import sqlalchemy
from sqlalchemy import event
from typing import Union, Dict, List
import configparser
import os
import importlib.util

import custom_template
from pathlib import Path

from dbflow.logging_config import logger



class RCMArchive:
    """
    A class to manage a Spatialite-enabled SQLite database.

    Attributes:
        archive (object): A namespace to store database components.
        session (Session): A SQLAlchemy session for transactions.

    Methods:
        attach_sqlalchemy_spatialite_listener()
        reflect_database(): Reflects the current database schema into SQLAlchemy metadata.
        add_tables(tables): Adds new tables to the database if they do not exist.
        prepare_database(): Prepares ORM mapping using SQLAlchemy's automap.
        close(): Closes the database connection and session cleanly.
    """

    def __init__(self, dbfile, **kwargs):
        """Initializes the RCMArchive with a specified database file and loads SpatiaLite."""

        self.archive = type('Archive', (object,), {})()
        self.archive.dbfile = dbfile
        # self.spatialite_dll = spatialite_dll
        self.archive.conn = sqlite3.connect(self.archive.dbfile)

        # Create SQLAlchemy engine and session
        self.archive.engine = create_engine(f'sqlite+pysqlite:///{self.archive.dbfile}',
                                            connect_args={'check_same_thread': False}, **kwargs)
        # self.archive.meta = MetaData(bind=self.archive.engine)
        self.archive.meta = MetaData()  # In SQLAlchemy 2.x, the bind parameter for MetaData was removed.
        self.session = sessionmaker(
            bind=self.archive.engine)()  # Sets up the session for database transactions - to perform CRUD (Create, Read, Update, Delete)

        # Attach SpatiaLite loader for SQLAlchemy connections # will link to spatialite at each activated connection
        self.attach_sqlalchemy_spatialite_listener()

        # Reflect database schema and prepare mapping
        self.reflect_database()
        self.prepare_database()

        # Add tables if needed
        tables = tables_to_create()
        self.add_tables(tables)

        # Assign the function to the archive object
        self.archive.get_tablenames = self.get_tablenames
        self.archive.get_colnames = self.get_colnames


    def attach_sqlalchemy_spatialite_listener(self):
        """
        Ensures that the SpatiaLite extension is properly linked at each SQLAlchemy connection.
        """
        conda_env_path = os.environ.get('CONDA_PREFIX')
        spatialite_path = os.path.join(conda_env_path, 'Library', 'bin', 'mod_spatialite.dll')

        @event.listens_for(self.archive.engine, "connect")
        def load_spatialite_extension(dbapi_connection, connection_record):
            try:
                # Ensure the extension is loaded for the SQLAlchemy connection
                dbapi_connection.enable_load_extension(True)
                dbapi_connection.load_extension(spatialite_path)
                logger.info("SpatiaLite extension loaded for SQLAlchemy connection.")

                # Verify spatial metadata exists
                cursor = dbapi_connection.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='geometry_columns';")
                exists = cursor.fetchone()

                if not exists:
                    logger.info("Spatial metadata missing in SQLAlchemy session. Initializing...")
                    cursor.execute("SELECT InitSpatialMetaData(1);")
                    dbapi_connection.commit()
                    logger.info("SpatiaLite metadata initialized successfully for SQLAlchemy session.")

                cursor.close()

            except Exception as e:
                logger.error(f"Failed to load SpatiaLite for SQLAlchemy: {e}")
                raise e


    def test_spatial_functionality(self):
        """Test spatial functions to ensure SpatiaLite is configured correctly."""

        try:
            # Run a spatial operation (SpatiaLite functions should already be available)
            result = self.archive.conn.execute(
                "SELECT ST_AsText(ST_Transform(MakePoint(0, 0, 4326), 32633));"
            ).fetchone()
            logger.info(f"Spatial function test result: {result}")

        except sqlite3.OperationalError as e:
            logger.error(f"Spatial function test failed: {e}")
            if "no such function" in str(e):
                logger.error("Error: SpatiaLite extension is not loaded or recognized.")
            elif "proj_create" in str(e):
                logger.error("Hint: PROJ_LIB might not be correctly set for proj.db.")
            raise e


    def reflect_database(self):
        """Reflect the database schema into SQLAlchemy's metadata."""
        try:
            self.archive.meta.reflect(bind=self.archive.engine)
            logger.info("Database schema reflected successfully.")
        except Exception as e:
            logger.error(f"Failed to reflect database schema: {e}")


    def add_tables(self, tables):
        """Add new tables to the database if they do not exist."""
        inspector = sqlalchemy.inspect(self.archive.engine)  # Create the Inspector object
        existing_tables = inspector.get_table_names()  # Get list of existing tables

        for table in tables:
            if table.name not in existing_tables:
                table.create(self.archive.engine)  # Create the table if it doesn't exist
                logger.info(f"Table '{table.name}' created successfully.")
            else:
                logger.info(f"Table '{table.name}' already exists.")

    def create_table_from_sql(self, sql, table_name):
        """
        Executes a CREATE TABLE SQL statement for the given table,
        only if the table does not already exist.

        Parameters
        ----------
            sql (str): The CREATE TABLE SQL statement.
            table_name (str): Name of the table being created.

        Example
        -------
        >>> db = connect2db(r'...test\test.db')
        >>> engine = db.archive.engine
        >>> sql = create_sql(r"...test\ml_transferability.sql")
        >>> db.create_table_from_sql(sql, "ml_transferability")
        """
        from sqlalchemy import inspect
        try:
            inspector = inspect(self.archive.engine)
            existing_tables = inspector.get_table_names()

            if table_name in existing_tables:
                logger.info(f"Table '{table_name}' already exists. Skipping creation.")
                info = self.get_table_info(table_name)

                if info["exists"]:
                    print("Columns:", info["columns"])
                else:
                    print("Table does not exist.")
                return info

            with self.archive.engine.connect() as conn:
                conn.execute(text(sql))
                logger.info(f"Table '{table_name}' created successfully.")

        except Exception as e:
            logger.error(f"Error creating table '{table_name}': {e}")
            raise

    def get_table_info(self, table_name):
        '''
        Parameters
        ----------
            table_name (str):

        Returns
        -------
            dict
        '''
        try:
            col_names = self.archive.get_colnames(table_name)
            return {"exists": True, "columns": col_names}
        except Exception as e:
            logger.warning(f"Could not get columns for '{table_name}': {e}")
            return {"exists": False, "columns": []}


    # Define the get_tablenames method as a function that returns table names
    def get_tablenames(self):
        """Return a list of all table names in the database."""
        return [table.name for table in self.archive.meta.sorted_tables]


    # Define the get_colnames method to retrieve column names for a specific table
    def get_colnames(self, table):
        """Return a list of column names for the given table."""
        if table in self.archive.meta.tables:
            table_ = self.archive.meta.tables[table]
            return [column.name for column in table_.columns]
        else:
            raise ValueError(f"Table '{table}' does not exist in the database.")


    def get_geom_colnames(self, table=None) :
        """
        Retrieve all geometry columns for a specific table or for all tables in the database.

        Parameters
        ----------
        table : str, optional
            Name of the table to filter geometry columns for. If not specified, retrieves geometry columns for all tables.

        Returns
        -------
        list or dict
            - If `table` is specified, returns a list of geometry column names for that table.
            - If `table` is not specified, returns a dictionary with table names as keys and their geometry column names as values.
        """

        # Query the geometry_columns table for geometry column information
        geom_list = self.archive.conn.execute(
            'SELECT f_table_name, f_geometry_column FROM geometry_columns;'
        ).fetchall()

        # Create a dictionary to store table names and their corresponding geometry columns
        geom_dict = {}

        for row in geom_list:
            f_table_name, f_geometry_column = row
            geom_dict.setdefault(f_table_name, []).append(f_geometry_column)

        if table:
            # Return the list of geometry column names for the specified table
            return geom_dict.get(table, [])
        else:
            # Return the entire dictionary of geometry columns
            return geom_dict


    def get_primary_keys(self, table):
        """
        Retrieve the primary key columns of a specified table.

        Parameters
        ----------
        table : str
            Name of the table for which to retrieve primary key columns.

        Returns
        -------
        list of str
            A list containing the names of the primary key columns for the specified table.
            If the table has no primary key, an empty list is returned.

        Example
        -------
        >>> rcm_archive.get_primary_keys('example_table')
        ['id']  # Assuming 'id' is the primary key column for 'example_table'
        """
        return [key.name for key in Table(table, self.archive.meta,
                                          autoload=True,
                                          autoload_with=self.archive.engine).primary_key]


    def get_primary_keys(self, table):
        """
        Retrieve the primary key columns of a specified table.

        Parameters
        ----------
        table : str
            Name of the table for which to retrieve primary key columns.

        Returns
        -------
        list of str
            A list containing the names of the primary key columns for the specified table.
            If the table has no primary key, an empty list is returned.

        Example
        -------
        >>> rcm_archive.get_primary_keys('example_table')
        ['id']  # Assuming 'id' is the primary key column for 'example_table'
        """
        # Load table metadata
        table_meta = Table(table, self.archive.meta,  # autoload=True for old version of sqlalchemy <2.0
                           autoload_with=self.archive.engine
                           )

        # Retrieve primary key columns
        return [key.name for key in table_meta.primary_key]


    def prepare_database(self):
        """Prepare ORM mapping using SQLAlchemy's automap."""
        self.Base = automap_base(metadata=self.archive.meta)
        self.Base.prepare(engine=self.archive.engine, reflect=True)
        logger.info("ORM mapping prepared.")


    def close(self):
        """Cleanly close the database connection and session."""
        if self.session:
            self.session.close()
        if self.archive.conn:
            self.archive.conn.close()


    def check_table_exists(self, table: str) -> bool:
        """
        Check if the specified table exists in the database.

        Parameters
        ----------
        table: str
            Name of the table to check.

        Returns
        -------
        bool
            True if the table exists, False otherwise.
        """
        # tables = self.archive.get_tablenames(return_all=True)
        tables = self.archive.get_tablenames()
        if table not in tables:
            logger.info(f'Table {table} does not exist in the database {self.archive.dbfile}.')
            return False
        return True


    def check_table_empty(self, table: str) -> bool:
        """
        Check if the specified table is empty (i.e., contains no rows).

        Parameters
        ----------
        table : str
            Name of the table to check.

        Returns
        -------
        bool
            True if the table is empty, False if it contains any rows.

        Raises
        ------
        ValueError
            If the specified table does not exist in the database.
        """
        # Ensure the table exists
        if not self.check_table_exists(table):
            raise ValueError(f"Table '{table}' does not exist in the database.")

        # Execute query to check if any rows exist
        result = self.archive.conn.execute(f'SELECT 1 FROM {table} LIMIT 1').fetchone()

        # Return True if no rows were found, False otherwise
        if result is None:
            logger.info(f'Table {table} is empty!')
            return True
        return False


    '''
    def get_intersection(self, vectorobject, geom_col_name):
        """
        Perform an intersection of a vector object with a WKT geometry.

        Parameters
        ----------
        vectorobject : str or geopandas.GeoDataFrame
            A file path to a vector file or a GeoDataFrame.
        geom_col_name : str
            The name of the geometry column in the database table.

        Returns
        -------
        None
            Modifies arg_format and vals in place based on the intersection check.
        """
        arg_format = []
        vals = []

        # Load vector data using geopandas
        if isinstance(vectorobject, str):
            vector_gdf = gpd.read_file(vectorobject)
        elif isinstance(vectorobject, gpd.GeoDataFrame):
            vector_gdf = vectorobject
        else:
            print('WARNING: argument vectorobject is ignored. Must be a path to a vector file or a GeoDataFrame.')
            return

        # Reproject to WGS84 if needed
        if vector_gdf.crs != "EPSG:4326":
            vector_gdf = vector_gdf.to_crs("EPSG:4326")

        # Get bounding box of all features and convert to WKT
        bbox = vector_gdf.total_bounds  # [minx, miny, maxx, maxy]
        site_geom = box(*bbox).wkt  # Convert bounding box to WKT

        # PostgreSQL vs. SpatiaLite syntax
        if self.archive.driver == 'postgres':
            arg_format.append("st_intersects({0}, 'SRID=4326; {1}')".format(
                geom_col_name,
                site_geom
            ))
        else:
            arg_format.append('st_intersects(GeomFromText(?, 4326), {}) = 1'.format(geom_col_name))
            vals.append(site_geom)

        # Log for debugging
        print('site_geom WKT:', site_geom)

        # Return or modify other variables as needed (e.g., arg_format and vals)
        return arg_format, vals
    '''

    # before it was query_db()
    def fetch_records(self, table, selected_columns='*', vectorobject=None, date=None, **args):
        """
        Select entries from the database based on various filters and spatial constraints.

        Parameters
        ----------
        table : str
            The table to select from. Available names can be checked via `self.archive.get_tablenames()`.
        selected_columns : list or str, optional
            List of columns to retrieve, or '*' for all columns. Default is '*'.
        vectorobject : str or geopandas.GeoDataFrame, optional
            A path to a vector file or a GeoDataFrame to be used for spatial intersection.
            Only one geometry column should be selected if this parameter is used.
        date : str or list, optional
            A single date or a date range [start, end] for filtering.
        **args : additional filters
            Additional filters where keys represent column names and values represent filter values.

        Returns
        -------
        list of dict
            A list of dictionaries where each dictionary represents a row in the result.

        """
        # Check if the table exists
        if not self.check_table_exists(table):
            logger.info(f"The table '{table}' does not exist.")
            return []

        # Check if the table is empty
        if self.check_table_empty(table):
            logger.info(f"The table '{table}' is empty.")
            return []

        # Get all column names of the table
        col_names = self.archive.get_colnames(table)

        # Determine selected columns
        if selected_columns == '*':
            selected_columns_list = col_names
        else:
            selected_columns = [selected_columns] if isinstance(selected_columns, str) else selected_columns
            selected_columns_list = [col for col in selected_columns if col in col_names]

        # Check if a geometry column is among selected columns
        geomcolname_list = self.get_geom_colnames(table=table)
        if len(geomcolname_list) > 1:
            logger.info(f"WARNING: Table '{table}' contains more than one geometry column.")
        geom_col_name = geomcolname_list[0] if geomcolname_list else None

        # Prepare geometry columns as WKT if needed
        if geom_col_name and geom_col_name in selected_columns_list:
            col_names_without_geom = [col for col in selected_columns_list if col not in geomcolname_list]
            geom_sqltxt = [f"AsText({geom}) AS {geom}" for geom in geomcolname_list]
            selected_columns_list = col_names_without_geom + geom_sqltxt

        # Filter out invalid selected columns
        invalid_columns = [col for col in selected_columns if col not in col_names]
        if invalid_columns:
            logger.info(f"Ignoring invalid columns not found in '{table}': {', '.join(invalid_columns)}")

        # Date filtering if a date or date range is provided
        date_conditions = []
        if isinstance(date, list) and len(date) == 2:
            date_conditions.append(f"date >= '{date[0]}' AND date <= '{date[1]}'")
        elif isinstance(date, str):
            date_conditions.append(f"date = '{date}'")

        # Additional filters from **args
        arg_conditions = []
        vals = []
        for key, value in args.items():
            if key in col_names:
                if isinstance(value, (float, int, str)):
                    arg_conditions.append(f"{key} = ?")
                    vals.append(value)
                elif isinstance(value, (tuple, list)):
                    placeholders = ', '.join(['?' for _ in value])
                    arg_conditions.append(f"{key} IN ({placeholders})")
                    vals.extend(value)
            else:
                logger.info(f"WARNING: Ignoring invalid filter for '{key}' as it is not in '{table}' columns.")

        # Spatial filtering with vectorobject if provided
        if vectorobject and geom_col_name:
            # Load vector data using geopandas if it’s a file path or GeoDataFrame
            vector_gdf = gpd.read_file(vectorobject) if isinstance(vectorobject, str) else vectorobject
            if vector_gdf.crs != "EPSG:4326":
                vector_gdf = vector_gdf.to_crs("EPSG:4326")
            bbox = box(*vector_gdf.total_bounds).wkt

            if self.archive.driver == 'postgres':
                arg_conditions.append(f"st_intersects({geom_col_name}, 'SRID=4326; {bbox}')")
            else:
                arg_conditions.append(f"st_intersects(GeomFromText(?, 4326), {geom_col_name}) = 1")
                vals.append(bbox)

        # Construct the SQL query
        query = f"SELECT {', '.join(selected_columns_list)} FROM {table}"
        all_conditions = arg_conditions + date_conditions
        if all_conditions:
            query += f" WHERE {' AND '.join(all_conditions)}"

        logger.info("Executing query:", query)

        # Execute the query and fetch results
        query_rs = self.archive.conn.execute(query, vals)
        columns = [col[0] for col in query_rs.description]
        return [dict(zip(columns, row)) for row in query_rs.fetchall()]


    '''
    def __prepare_insert(self, table, verbose=True, **args):
        """
        generic input string generator for tables

        Parameters
        ----------
        table: str
            table for which insertion string should be created
        verbose: bool
            print additional info

        Returns
        -------
        insertion string
        """
        if self.check_table_exists(table):
            col_names = self.archive.get_colnames(table)
            arg_invalid = [x for x in args.keys() if x not in col_names]
            if len(arg_invalid) > 0:
                if verbose:
                    print('Following arguments {} were not ingested in table {}'.format(', '.join(arg_invalid), table))
            stmt = self.archive.meta.tables[table].insert().values(**args)
            compiled_statement = stmt.compile(dialect=sqlite.dialect())
            original_statement = compiled_statement.string
            print(compiled_statement)
            print(args)
            binds = compiled_statement.binds
            for k, v in binds.items():
                print(k, v)

            # for k, v in args.items():
            # print(k, v)
            #    print(k, v, type(v))
            return self.archive.meta.tables[table].insert().values(**args)
    '''
    '''
    def __prepare_insert(self, table, **args):
        """
        generic input string generator for tables

        Parameters
        ----------
        table: str
            table for which insertion string should be created

        Returns
        -------
        insertion string
        """
        if self.check_table_exists(table):
            col_names = self.archive.get_colnames(table)
            arg_invalid = [x for x in args.keys() if x not in col_names]
            if len(arg_invalid) > 0:
                print(f'Following arguments {", ".join(arg_invalid)} were not ingested in table {table}')

            # Print column names and their corresponding values (for debugging)
            print(f'Inserting into table {table}:')
            for key, value in args.items():
                print(f'{key}: {value} (Type: {type(value)})')

                # Validate if the column expects a float and if value is not convertible
                if 'parameter_value' == key:  # Adjust this to your column's actual name
                    if value is None:
                        print(f'{key} is None, setting to -999')
                        value = -999  # Assign default value for None
                    else:
                        try:
                            value = float(value)  # Attempt to convert to float if it's not None
                        except ValueError:
                            print(f'Invalid float value for {key}: {value}, setting to -999')
                            value = -999  # Assign default value for invalid floats

                # Update the value in the args dictionary
                args[key] = value

            # Generate the insert statement
            stmt = self.archive.meta.tables[table].insert().values(**args)
            compiled_statement = stmt.compile(dialect=sqlite.dialect())
            print(f'Compiled statement: {compiled_statement}')
            print(f'Bind parameters: {compiled_statement.binds}')

            return stmt


    def __prepare_upsert(self, table, primary_key, **args):
        """
        generic update string generator for tables

        Parameters
        ----------
        table: str
            table for which insertion string should be created
        primary_key: list of str
            primary key of table within list, or combined key as list of keys

        Returns
        -------
        insertion string
        """

        # MAIN Source !!: https://github.com/sqlalchemy/sqlalchemy/issues/4010
        # https://www.postgresqltutorial.com/postgresql-upsert/

        if self.check_table_exists(table):
            col_names = self.archive.get_colnames(table)
            arg_invalid = [x for x in args.keys() if x not in col_names]
            if len(arg_invalid) > 0:
                print('Following arguments {} were not ingested in table {}'.format(', '.join(arg_invalid), table))

            # create an insert statement
            insert_stmt = self.archive.meta.tables[table].insert().values(**args)

            # extract the column names of the existing table
            table_column_info = self.archive.meta.tables[table].c

            # restore args in right order of the db table
            # args_corrected = {}
            # for col in table_column_info:
            #    print(col.name, col.type)
            #     args_corrected[col.name] = args[col.name]

            # compile the statement so that we can get all the bound parameters
            compiled_statement = insert_stmt.compile(
                dialect=sqlite.dialect(paramstyle='named'))  # paramstyle="named" or 'qmark'

            # binds is a mapping of {parameter_name -> BindParameter} e.g. {"id_1": BindParameter("id_1", value=1, type_=Integer)}
            # binds = compiled_statement.binds
            params = compiled_statement.params
            # print(*params.values())

            # we want the str SQL statement to edit
            original_statement = compiled_statement.string

            # collect columns that are not primary keys
            excluded_keys = []
            for key, value in args.items():
                if key not in primary_key:
                    excluded_keys.append(key + "=" + "EXCLUDED." + key)

            # and we will hand build our custom UPSERT statement
            upsert_stmt = "ON CONFLICT (" + ', '.join(primary_key) + ") DO UPDATE SET " + ', '.join(excluded_keys) + ";"

            # before putting it all together
            stmt = " ".join([original_statement, upsert_stmt])
            # print(text(stmt))
            stmt = text(stmt).bindparams(**params)
        return stmt

    '''


    def __prepare_insert(self, table, **args):
        """
        Generate a generic SQL insertion statement for a specified table.

        Parameters
        ----------
        table : str
            The name of the table to insert data into.
        **args : dict
            Column-value pairs for data insertion. Ensure that each column exists in the table.

        Returns
        -------
        sqlalchemy.sql.expression.Insert
            An SQLAlchemy Insert statement object with bound parameters.

        Example
        -------
        >>> __prepare_insert('example_table', column1='value1', column2='value2')
        """
        # Verify if table exists
        if not self.check_table_exists(table):
            logger.info(f"Table '{table}' does not exist.")
            return None

        # Retrieve column names
        col_names = self.archive.get_colnames(table)
        arg_invalid = [x for x in args.keys() if x not in col_names]
        if arg_invalid:
            logger.info(f"Following arguments {', '.join(arg_invalid)} were not ingested in table '{table}'.")

        # Validate and adjust input values
        for key, value in args.items():
            if key == 'parameter_value':  # Modify this to match your column name
                if value is None:
                    logger.info(f"{key} is None, setting to default -999.")
                    value = -999
                else:
                    try:
                        value = float(value)
                    except ValueError:
                        logger.info(f"Invalid float for {key}: {value}, setting to -999.")
                        value = -999
                args[key] = value  # Update with validated value

        # Generate insert statement
        stmt = self.archive.meta.tables[table].insert().values(**args)
        compiled_statement = stmt.compile(dialect=sqlite.dialect())
        #print(f"Compiled statement: {compiled_statement}")
        #print(f"Bind parameters: {compiled_statement.params}")
        return stmt


    def __prepare_upsert(self, table, primary_key, **args):
        """
        Generate a generic UPSERT (insert or update) SQL statement for a specified table.

        Parameters
        ----------
        table : str
            The name of the table for upsert operation.
        primary_key : list of str
            List of primary key columns for conflict resolution.
        **args : dict
            Column-value pairs for upserting data. Ensure that each column exists in the table.

        Returns
        -------
        sqlalchemy.sql.expression.TextClause
            A SQLAlchemy TextClause object representing the UPSERT statement.

        Example
        -------
        >>> __prepare_upsert('example_table', primary_key=['id'], column1='value1', column2='value2')
        """
        # Verify if table exists
        if not self.check_table_exists(table):
            logger.info(f"Table '{table}' does not exist.")
            return None

        # Retrieve and validate column names
        col_names = self.archive.get_colnames(table)
        arg_invalid = [x for x in args.keys() if x not in col_names]
        if arg_invalid:
            logger.info(f"Following arguments {', '.join(arg_invalid)} were not ingested in table '{table}'.")

        # Create an insert statement
        insert_stmt = self.archive.meta.tables[table].insert().values(**args)
        compiled_statement = insert_stmt.compile(dialect=sqlite.dialect(paramstyle='named'))
        params = compiled_statement.params

        # Build custom UPSERT statement
        excluded_keys = [f"{key}=EXCLUDED.{key}" for key in args.keys() if key not in primary_key]
        upsert_stmt = f"ON CONFLICT ({', '.join(primary_key)}) DO UPDATE SET {', '.join(excluded_keys)};"

        # Combine insert statement with upsert clause
        stmt = " ".join([compiled_statement.string, upsert_stmt])
        stmt = text(stmt).bindparams(**params)

        #print(f"Upsert statement: {stmt}")
        #print(f"Parameters: {params}")
        return stmt


    def insert(self, table, primary_key, orderly_data, update=False):
        """ Generic inserter for tables, checks if entry is already in db. """
        session = self.session
        rejected = []

        try:
            # Reflect table schema only once
            table_info = Table(table, self.archive.meta, autoload=True, autoload_with=self.archive.engine)

            for entry in orderly_data:
                if update:
                    session.execute(self.__prepare_upsert(table, primary_key, **entry))
                else:
                    exists_str = exists()
                    for p_key in primary_key:
                        exists_str = exists_str.where(table_info.c[p_key] == entry[p_key])
                    ret = session.query(exists_str).scalar()

                    if ret:
                        rejected.append(entry)
                    else:
                        session.execute(self.__prepare_insert(table, **entry))

            session.commit()  # Commit all changes at once
        except Exception as e:
            session.rollback()  # Rollback in case of error
            logger.info(f"Error during insertion: {e}")
        finally:
            session.close()  # Always close the session

        message = f'Ingested {len(orderly_data) - len(rejected)} entries to table {table}'
        if rejected:
            logger.info(f'Rejected entries with already existing primary key: {rejected}')
            message += f', rejected {len(rejected)} (already existing).'
        logger.info(message)


    '''
    def insert(self, table, primary_key, orderly_data, verbose=False, update=False):
        """
        generic inserter for tables, checks if entry is already in db,
        update can be used to overwrite all concerning entries

        Parameters
        ----------
        table: str
            table in which data insertion should be
        primary_key: list of str
            primary key of table within list, or combined key as list of keys
        orderly_data: list of dicts
            list of dicts created by xx_01.loadwd.make_a_list
        verbose: bool
            print additional info
        update: bool
            update database? will overwrite all entries given in orderly_data

        Returns
        -------

        """
        session = self.archive.Session()

        # self.Base = automap_base(metadata=self.archive.meta) # this does not work for insert of data more than 600 rows
        # self.Base.prepare(self.archive.engine, reflect=True) # this does not work for insert of data more than 600 rows

        self.check_table_exists(table)
        table_info = Table(table, self.archive.meta, autoload=True, autoload_with=self.archive.engine)
        # session = self.archive.Session()

        # ------- check column types  TODO: implement check if column is geometry and implement Geometry2 wrapp!! see __refactor_sentinel2data()
        # table_column_info = Table(table, self.archive.meta, autoload=True, autoload_with=self.archive.engine).c
        # coltypes = {}
        # for i in table_column_info:
        #    coltypes[i.name] = i.type
        #    print(i.name, i.type)

        # ----------------------------------

        rejected = []

        # session could be used here maybe..
        # session = self.archive.Session()
        # tableobj = self.Base.classes[table] # this does not work for insert of data more than 600 rows

        for entry in orderly_data:
            if update:
                # print(entry)
                # print(len(entry))
                self.archive.conn.execute(self.__prepare_upsert(table, primary_key, **entry))
                # session.merge(tableobj(**entry)) # this does not work for insert of data more than 600 rows
                # session.commit()
            else:
                exists_str = exists()
                for p_key in primary_key:
                    exists_str = exists_str.where(table_info.c[p_key] == entry[p_key])
                ret = session.query(exists_str).scalar()

                if ret:
                    rejected.append(entry)
                else:
                    print(table)
                    self.archive.conn.execute(self.__prepare_insert(table, **entry))
                    # II variante insert data # this does not work for insert of data more than 600 rows
                    # session.add(tableobj(**entry))
                    # session.commit()
        # session.close()
        message = 'Ingested {} entries to table {}'.format(len(orderly_data) - len(rejected), table)
        if len(rejected) > 0:
            if verbose:
                print('Rejected entries with already existing primary key: ', rejected)
            message += ', rejected {} (already existing).'.format(len(rejected))
        print(message)
        session.close()
    '''


def tables_to_create_():
    """
    Dynamically retrieve all table classes from db_structure

    Parameters
    ----------

    Returns
    -------
    list
        list of names of table classes
    """

    tables = []

    '''
    #add tables from the main module to the list to be created
    for name, cls in inspect.getmembers(importlib.import_module('db_structure'), inspect.isclass):
        print(name, cls)
        if cls.__module__ == 'db_structure':
            tables.append(eval(name).__table__)
    '''

    # Import the db_structure module dynamically
    try:
        db_structure_module = importlib.import_module('dbflow.db_structure')
    except ModuleNotFoundError as e:
        logger.warning(f"Module not found: {e}")
        return tables

    # Add tables from the db_structure module
    for name, cls in inspect.getmembers(db_structure_module, inspect.isclass):
        logger.info(f"Class Name: {name}, Class: {cls}")
        logger.info(f"Class Module: {cls.__module__}")

        # Check if the class belongs to the 'src.db_structure' module
        if cls.__module__ == 'dbflow.db_structure':
            # Safely get the table associated with the class, if it exists
            if hasattr(cls, '__table__'):
                logger.info(f"Table for {name}: {cls.__table__}")
                tables.append(cls.__table__)
            else:
                logger.warning(f"Class {name} does not have a __table__ attribute")

    # Handle case where no tables are found
    if len(tables) == 0:
        logger.warning('ERROR: No tables found to create!')

    return tables


def tables_to_create():
    """
    Dynamically retrieve all table classes from db_structure.

    Returns
    -------
    list
        List of SQLAlchemy table objects to be created.
    """
    tables = []

    # Load the db_structure module dynamically
    db_structure_module = load_custom_structure()
    if not db_structure_module:
        logger.warning('ERROR: No db_structure module found! Falling back to default.')
        return tables

    # Iterate over classes in the db_structure module
    for name, cls in inspect.getmembers(db_structure_module, inspect.isclass):
        logger.info(f"Class Name: {name}, Class: {cls}")
        logger.info(f"Class Module: {cls.__module__}")

        # Check if the class belongs to the loaded db_structure module
        if cls.__module__ == db_structure_module.__name__:
            # Safely get the table associated with the class, if it exists
            if hasattr(cls, '__table__'):
                logger.info(f"Table for {name}: {cls.__table__}")
                tables.append(cls.__table__)
            else:
                logger.warning(f"Class {name} does not have a __table__ attribute")

    # Handle case where no tables are found
    if len(tables) == 0:
        logger.warning('ERROR: No tables found to create!')

    return tables



def connect2db(path):
    """
    Connect to the database or create it if it doesn't exist.

    Parameters:
        path (str): Full path to the SQLite database file (e.g., '../sqliteDB.db').

    Returns:
        RCMArchive: An instance of RCMArchive connected to the specified database.
    """
    db_path = Path(path)

    # Ensure the parent directory exists, if not, create it
    db_path.parent.mkdir(parents=True, exist_ok=True)

    return RCMArchive(db_path.as_posix())


def create_sql_(sql_file='main_query_datatypes_barchart.sql', replacements=None, write_sql=True):
    """
    Dynamically loads an SQL file and replaces placeholders with dynamic values.

    Parameters
    ----------
    sql_file : str
        Name of the SQL file to load.
    replacements : dict
        Dictionary of placeholders (keys) and their replacement values (values).
    write_sql : bool
        If True, writes the rendered SQL query to the '_sql_executed' directory.

    Returns
    -------
    str
        The rendered SQL query as a string.
    """
    # Directories to search for the SQL file
    project_root = Path(__file__).parent.resolve().parent.parent
    base_dirs = [
        project_root / 'custom/sql',
        project_root / 'custom_template/sql',
    ]

    logger.info(f"Looking for '{sql_file}' in the following directories:")
    for base_dir in base_dirs:
        logger.info(base_dir.resolve())

    # Attempt to load the SQL file
    res_file = None
    for base_dir in base_dirs:
        sql_file_path = base_dir / sql_file
        if sql_file_path.exists():
            res_file = sql_file_path.read_text(encoding='utf-8')
            break

    if res_file is None:
        available_files = {str(dir): [f.name for f in dir.glob('*.sql')] for dir in base_dirs if dir.exists()}
        raise FileNotFoundError(
            f"SQL file '{sql_file}' not found in any of the following directories: {', '.join(map(str, base_dirs))}. "
            f"Available files: {available_files}"
        )

    # Replace placeholders with dynamic values
    if replacements is not None:
        for placeholder, value in replacements.items():
            res_file = res_file.replace(placeholder, value)

    # Write the rendered SQL query to '_sql_executed'
    if write_sql:
        executed_sql_dir = Path('./_sql_executed')
        executed_sql_dir.mkdir(parents=True, exist_ok=True)

        executed_file_path = executed_sql_dir / sql_file
        executed_file_path.write_text(res_file, encoding='utf-8')

    return res_file


def generate_internal_temp_folder(folder_name):
    f_dir = Path(custom_template.sql.__file__).parent.parent.parent.parent.joinpath(folder_name)
    try:
        f_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        logger.info("'{}' folder is already there".format(folder_name))
    else:
        logger.info("'{}' folder was created".format(folder_name))
    return f_dir


def query_sql(sql, db_engine):
    """
    Execute an SQL query and return the results as a pandas DataFrame.

    This function sends an SQL query to the specified database and retrieves the results
    in the form of a pandas DataFrame. If the query returns no results, a log message is
    generated to notify the user.

    Parameters
    ----------
    sql : str
        The SQL query to execute. Must be a valid SQL statement supported by the database.

    db_engine : object
        Database connection object. Can be a DBAPI2 connection (e.g., sqlite3, psycopg2)
        or an SQLAlchemy engine/connection.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the results of the SQL query. If no rows are returned,
        the DataFrame will be empty.

    Raises
    ------
    ValueError
        If the provided database connection is not valid or does not support SQL queries.

    Notes
    -----
    - This function expects `db_engine` to be a valid connection object that supports querying
      using `pandas.read_sql()`.
    - If the query returns no data, the function logs an informational message
      but still returns an empty DataFrame.

    Examples
    --------
    Query data from a users table:

    >>> import sqlite3
    >>> conn = sqlite3.connect('example.db')
    >>> sql = "SELECT * FROM users"
    >>> df = query_sql(sql, conn)
    >>> print(df.head())

    Query data using SQLAlchemy engine:

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('sqlite:///example.db')
    >>> df = query_sql("SELECT * FROM orders", engine)
    >>> print(df.info())
    """
    try:
        data = pd.read_sql(sql, db_engine)

        if data.empty:
            logger.warning('Query returned empty! Table is empty or SQL query is incorrect.')
        return data

    except Exception as e:
        logger.error(f"Error executing SQL query: {e}")
        raise



def get_custom_paths():
    """
    Searches for config.ini in multiple directory levels (app directory and parents).
    Falls back to default paths in the dbflow package if config.ini is not found.

    Returns
    -------
    dict
        A dictionary containing paths for custom SQL and database structure.
    """
    # Start searching from the current working directory
    app_root = Path.cwd()

    # Search for config.ini in current and parent directories (up to 3 levels)
    config_path = None
    for _ in range(6):  # Search up to 5 parent directories
        possible_path = app_root / "config.ini"
        if possible_path.exists():
            config_path = possible_path
            break  # Stop searching once found
        app_root = app_root.parent  # Move one level up

    # Log the found config file or warning if missing
    if config_path:
        logger.info(f"Found config.ini at: {config_path}")
        config_dir = config_path.parent
    else:
        logger.warning("Config file not found. Using default paths.")
        config_dir = None

    # Default paths (fallback to dbflow's template if config.ini is missing)
    package_dir = Path(__file__).resolve().parent.parent.parent
    default_paths = {
        'custom_sql_dir': package_dir / 'custom_template/sql',
        'custom_db_structure': package_dir / 'custom_template/db_structure.py',
    }

    # If no config file, return default paths
    if not config_path:
        return default_paths

    # Read config.ini
    config = configparser.ConfigParser()
    config.read(config_path)

    return {
        #'custom_sql_dir': Path(config.get("paths", "custom_sql_dir", fallback=str(default_paths['custom_sql_dir']))),
        'custom_sql_dir':(config_dir / config.get("paths", "custom_sql_dir", fallback="custom_template/sql")).resolve(),
        #'custom_db_structure': Path(config.get("paths", "custom_db_structure", fallback=str(default_paths['custom_db_structure']))),
        'custom_db_structure': (config_dir / config.get("paths", "custom_db_structure", fallback="custom_template/db_structure.py")).resolve()
    }


def create_sql(sql_file, replacements=None, write_sql=True):
    """
    Dynamically loads an SQL file and replaces placeholders with dynamic values.
    """
    paths = get_custom_paths()
    custom_sql_dir = paths['custom_sql_dir']

    # Resolve the full path to the SQL file
    sql_file_path = custom_sql_dir / sql_file

    if not sql_file_path.exists():
        available_files = [f.name for f in custom_sql_dir.glob('*.sql')]
        raise FileNotFoundError(
            f"SQL file '{sql_file}' not found in directory: {custom_sql_dir}. "
            f"Available files: {available_files}"
        )

    # Read and replace placeholders
    sql = sql_file_path.read_text(encoding='utf-8')
    if replacements:
        for placeholder, value in replacements.items():
            sql = sql.replace(placeholder, value)

    # Optionally write the executed SQL to a temporary directory
    if write_sql:
        #app_root = Path(os.getcwd())  # Base the path on the current working directory
        #executed_dir = app_root.parent / '_sql_executed'

        paths = get_custom_paths() # make it customizable for a user, define the path in .ini file
        executed_dir = Path(paths.get('executed_sql_dir', '../_sql_executed')).resolve()

        executed_dir.mkdir(parents=True, exist_ok=True)

        executed_file_path = executed_dir / sql_file
        executed_file_path.write_text(sql, encoding='utf-8')

    return sql


def load_custom_structure():
    """
    Dynamically loads the custom db_structure.py script based on the paths provided in config.ini.

    Returns
    -------
    module or None
        The loaded custom db_structure module, or None if the file does not exist.
    """
    # Resolve custom paths
    paths = get_custom_paths()
    custom_db_structure_path = Path(paths['custom_db_structure']).resolve()

    # Check if the custom db_structure.py exists
    if custom_db_structure_path.exists():
        try:
            # Load the module dynamically
            spec = importlib.util.spec_from_file_location("custom.db_structure", str(custom_db_structure_path))
            custom_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(custom_module)
            logger.info(f"Loaded custom db_structure.py from {custom_db_structure_path}")
            return custom_module
        except Exception as e:
            logger.warning(f"Error loading custom db_structure.py: {e}")
            return None
    else:
        logger.info(f"Custom db_structure.py not found at {custom_db_structure_path}. Using default behavior.")
        return None


def load_sql_file(file_name):
    """
    Loads the content of an SQL file from the custom SQL directory.

    Parameters
    ----------
    file_name : str
        Name of the SQL file to load.

    Returns
    -------
    str
        The content of the SQL file as a string.

    Raises
    ------
    FileNotFoundError
        If the SQL file does not exist in the custom directory.
    """
    # Resolve the custom SQL directory from the config
    paths = get_custom_paths()
    custom_sql_dir = Path(paths['custom_sql_dir'])

    # Resolve the full path to the SQL file
    sql_file_path = custom_sql_dir / file_name

    # Check if the SQL file exists
    if not sql_file_path.exists():
        raise FileNotFoundError(f"SQL file '{file_name}' not found in '{custom_sql_dir}'.")

    # Read and return the content of the SQL file
    return sql_file_path.read_text(encoding='utf-8')


