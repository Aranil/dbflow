# -*- coding: utf-8 -*-
"""
@author: Aranil

Additional custom setup for working with RCM_DataBase (complementary file to 'db_structure.py' file)

Additional Info:
# https://stackoverflow.com/questions/58668255/is-there-an-example-sqlalchemy-userdefinedtype-for-microsoft-sql-server-geograph

"""

from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, BLOB  # datentypen table columns
#from geoalchemy2 import Geometry, WKTElement  # tabelle mit geometrien
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import UserDefinedType
#from geoalchemy2 import func
from sqlalchemy import func
from dateutil.parser import parse
import numpy as np
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
import shapely
from shapely import wkt
import sys
import pandas as pd
from dateutil.parser import parse, ParserError

from shapely.geometry import Polygon, MultiPolygon, Point


def convert_3D_2D(p):
    '''
    Takes a  Multi/Polygons Z (3D) and returns as  Multi/Polygons (2D)
    Source: https://stackoverflow.com/questions/33417764/basemap-readshapefile-valueerror/35211729#35211729

    '''

def _to_2d(x, y, z):
    """
    functionality to drop 3rd dimension
    """
    # Source: https://github.com/Toblerity/Shapely/issues/709
    return tuple(filter(None, [x, y]))

class Geometry2(UserDefinedType):
    """
    wrapper for geoalchemy2.Geometry() class to  import values into DB
    overriding of some methods of the Geometry class to be able marshal input data into a format appropriate
    for passing into the relational database (works on Windows(!), Linux(?) )

    https://cassiopeia.readthedocs.io/en/v0.1.1/_modules/sqlalchemy/sql/type_api.html
    https://docs.sqlalchemy.org/en/13/core/custom_types.html
    https://readthedocs.org/projects/geoalchemy-2/downloads/pdf/latest/
    https://github.com/zzzeek/sqlalchemy/blob/master/examples/postgis/postgis.py

    Examp:

        To import Polygon AS WKT value into created column of type Geometry:
                bbox = Column(Geometry('POLYGON', management=True, srid=4326)),
        use
                ...
                from rcm.archive import db_sqlalchemy as cstm
                temp_dict[key] = cstm.Geometry2.bind_expression(cstm.Geometry2(srid=4326), bindvalue=Polygon_AS_WKT_value)

    """

    cache_ok = True  # Enables caching for instances of this type


    def __init__(self, srid=4326):
        self.srid = srid

    def get_col_spec(self):
        return 'GEOMETRY'
        #return '%s(%s, %d)' % (self.name, self.geometry_type, self.srid)

    def bind_expression_(self, bindvalue):
        ## convert str wkt to  <class 'shapely.geometry.polygon.Polygon'>
        if isinstance(bindvalue, str):
            bindval = shapely.wkt.loads(bindvalue)
            #print('bindval: ', type(bindval))
        elif (isinstance(bindvalue, shapely.geometry.polygon.Polygon)):
            bindval = bindvalue
            print('bindval geom_polygon: ', type(bindval))
            print(bindval.wkt)
            print(type(bindval.wkt))
        elif (isinstance(bindvalue, shapely.geometry.point.Point)):
            bindval = bindvalue
            print('bindval geom_polygon: ', type(bindval))
            print(bindval.wkt)
            print(type(bindval.wkt))
        else:
            print(type(bindvalue))
            raise IOError('Shape is not a shapely type of polygon.')


            # import to db only as 2D geometry allowed,
        # if POLYGON Z .. (3D geometry) exist this will convert to POLYGON ... (2D geometry) => https://github.com/Toblerity/Shapely/issues/709
        if bindval.has_z:
            return func.ST_GeomFromEWKT('SRID=%d;%s' % (self.srid, shapely.ops.transform(_to_2d, bindval)), type_=self)
            #return func.ST_GeomFromEWKT('SRID=%d;%s' % (self.srid, shapely.ops.transform(_to_2d, bindval.wkt)), type_=self)
        else:
            # wrapper used during the data input into DB, value will be wrapped into some spatial database function (func.ST_GeomFromText)
            #return func.ST_GeomFromText(bindvalue, type_=self) # to import WKT without SRID (in this case srid=0 => not defined)
            return func.ST_GeomFromEWKT('SRID=%d;%s' % (self.srid, bindval.wkt), type_=self)

    def bind_expression(self, bindvalue):
        # Convert WKT string to Shapely geometry if needed
        if isinstance(bindvalue, str):
            bindval = wkt.loads(bindvalue)
            #print('bindval from WKT:', type(bindval))
        elif isinstance(bindvalue, Polygon):
            bindval = bindvalue.wkt
            print('bindval Polygon:', type(bindval))
            #print(bindval.wkt)
        elif isinstance(bindvalue, MultiPolygon):
            bindval = bindvalue.wkt
            print('bindval MultiPolygon:', type(bindval))
            #print(bindval.wkt)
        elif isinstance(bindvalue, Point):
            bindval = bindvalue.wkt
            print('bindval Point:', type(bindval))
            #print(bindval.wkt)
        else:
            #print('Invalid type:', type(bindvalue))
            raise IOError('Shape is not a supported Shapely geometry type (Polygon, MultiPolygon, or Point).')

        return bindval

    def column_expression(self, col):
        #  wrapper, used during SELECT statement
        #return func.ST_AsText(col, type_=self) # no srid defined srid=0
        return func.ST_AsEWKT(col, type_=self)


def sqldat_converter(x, to='datetime'):
    """
    Converts a given value to a SQLAlchemy datetime, date, or time function, based on the `to` argument.

    Parameters
    ----------
    x : any
        The value to convert; typically a string representing a date, datetime, or time.
    to : str, optional
        Target conversion type, either 'datetime', 'date', or 'time'. Default is 'datetime'.

    Returns
    -------
    SQLAlchemy function or None
        Returns a SQLAlchemy datetime, date, or time function, or None if the input is invalid or 'nan'.

    Notes
    -----
    If the input `x` is 'nan', 'NaN', `np.nan`, or contains known invalid patterns, returns None.
    """
    # Check for NaN or known invalid patterns
    if x in ['nan', 'NaN', np.nan] or pd.isnull(x) or 'datetime(:' in str(x):
        return None

    try:
        # Attempt to parse the datetime
        datetime_x = parse(str(x))
        if to == 'datetime':
            return func.datetime(datetime_x)
        elif to == 'date':
            return func.date(datetime_x)
        elif to == 'time' and hasattr(func, 'time'):  # Check if `func.time` exists
            return func.time(datetime_x)
        else:
            raise ValueError("Invalid conversion type specified. Use 'datetime', 'date', or 'time'.")
    except ParserError as e:
        # Return None or log a message if date format is invalid
        print(f"ParserError: Invalid date format for input {x}")
        return None

def sqldat_converter_(x, to='datetime'):
    if (x == 'nan') or (x == 'NaN') or (x == np.nan):
        x = None
        return x
    else:
        datetime_x = parse(str(x))
        if to == 'datetime':
            return func.datetime(datetime_x)
        elif to == 'date':
            return func.date(datetime_x)
        # find out if func.time exist ????
        elif to == 'time':
            return func.time(datetime_x)



#class MyEpochType(types.TypeDecorator):
#    impl = types.Integer
#
#    epoch = datetime.date(1970, 1, 1)
#
#    def process_bind_param(self, value, dialect):
#        return (value - self.epoch).days
#
#    def process_result_value(self, value, dialect):
#        return self.epoch + timedelta(days=value)



# EXAMPLES
# connect to existing RCM archive
#rcmarchive = db.RCMArchive(os.path.join(cfg.root, 'RCM26'))

#check=__check_table_exists(rcmarchive, table='s1grdzonalsatistic')

# drop table
#cfg.rcmarchive.archive.drop_table(table='s1grdzonalsatistic')

# create new table
#cfg.rcmarchive.archive.add_tables(S1GRDZonalSatistic.__table__)
