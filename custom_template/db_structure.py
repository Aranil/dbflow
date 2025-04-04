"""
DB Table structure definition

This file is Database and Project specific, change it or extend to generate custom tables
"""

from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, CLOB,  Text, UnicodeText, UniqueConstraint, ForeignKey, ForeignKeyConstraint
from sqlalchemy.orm import declarative_base
from geoalchemy2 import Geometry
from sqlalchemy.sql import func

from dbflow.logging_config import logger


Base = declarative_base()



class AreaOfInterest(Base):
    """
    Template for the AreOfInterset Table, Template for (RCM Project)
    """

    __tablename__ = 'areaofinterest'

    fid = Column(Integer, primary_key=True)  # id of the field polygon
    year = Column(Integer, primary_key=True)
    aoi = Column(String, primary_key=True)  # added info >> FRIEN, MRKN...
    sl_nr = Column(String, primary_key=True)  # shp atribute
    sl_name = Column(String)
    area = Column(Float)  # flaeche
    # crop_type = Column(String)                                         # Frucht (FRIEN), KULTUART (DEMM) Crop_2018 (MRKN)
    crop_type_code = Column(String)  # WW, WG...
    cultivar = Column(String)  # Sorte
    fid_gesamt = Column(Integer, primary_key=True)  # FID_Gesamt (FRIEN), fid (DEMM) fieldid (MRKN)
    field_geom = Column(Geometry('POLYGON', srid=4326))
    # field_geom_buffered = Column(Geometry('POLYGON', srid=4326, management=True))
    # seeding_date_from = Column(DateTime)                               # seeding (DEMM)
    # seeding_date_to = Column(DateTime)                                 # seeding2 (DEMM)
    # harvest_date_from = Column(DateTime)
    # harvest_date_to = Column(DateTime)
    comments = Column(String)  # Comments (DEMM)
    # row_direction_code = Column(String)                                # Anbaureihe (MRKN)
    # row_direction = Column(Geometry('LINESTRING', srid=4326, management=True))
    # aspect_code = Column(String)                                       # Himmelsric (MRKN)
    # aspect = Column(String)
    # code_color = Column(String)                                        # crop code for plotting
    file_header_name = Column(String)  # name of the header file
    datatype_code = Column(String)
    datatype_name = Column(String)
    datetime_inserted = Column(DateTime(timezone=True), server_default=func.current_timestamp())
    ForeignKeyConstraint(
        columns=['areaofinterest.aoi', 'areaofinterest.crop_type_code',
                 'areaofinterest.datatype_code', 'areaofinterest.datatype_name'],
        refcolumns=['aoilegend.aoi', 'croplegend.crop_type_code',
                    'datalegend.datatype_code', 'datalegend.datatype_name'],
        onupdate="CASCADE", ondelete="SET NULL")


