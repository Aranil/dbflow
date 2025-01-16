"""
@author: Aranil

DB Table structure definition

This file is Database and Project specific, change it or extend to generate custom tables
"""

from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, CLOB,  Text, UnicodeText, UniqueConstraint, ForeignKey, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
from sqlalchemy.sql import func

Base = declarative_base()

class APPobservations(Base):
    """
    Template for (AGRISENS Project)
    """

    __tablename__ = 'appobservations'

    #index = Column(String, primary_key=True)
    aoi = Column(String)
    id = Column(Integer)
    timestamp = Column(DateTime, primary_key=True)
    fieldname = Column(String, primary_key=True)
    fieldnumber = Column(Integer)
    user_id = Column(Integer)
    user_name = Column(String, primary_key=True)
    lat = Column(Float)
    long = Column(Float)
    alt = Column(Integer)
    acc = Column(Float)
    utc = Column(String)
    variable = Column(String, primary_key=True)
    value = Column(String)
    filename = Column(String)
    #imagepath = Column(String)  # to access the Image/Foto from the APP
    geometry = Column(Geometry('POINT', #management=True,
                               srid=4326))

    datatype_name = Column(String)
    datetime_inserted = Column(DateTime(timezone=True), server_default=func.current_timestamp())
    '''
    ForeignKeyConstraint(
        columns=['harvestinfo.sl_nr', 'harvestinfo.year',
                 'harvestinfo.aoi', 'harvestinfo.datatype_code',
                 'harvestinfo.datatype_name'],
        refcolumns=['appcode.value'],
        onupdate="CASCADE", ondelete="SET NULL")
    '''

class APPcode(Base):
    """
    Template for (AGRISENS Project)
    """

    __tablename__ = 'appcode'

    name = Column(String, primary_key=True)
    label_deu = Column(String, primary_key=True)
    label_eng = Column(String, primary_key=True)
    hint_deu = Column(String)
    hint_eng = Column(String)
    value = Column(String, primary_key=True)
    text_deu = Column(String)
    text_eng = Column(String)
    default_value = Column(String)
    type = Column(String)
    json_file = Column(String)

    datatype_name = Column(String)
    datetime_inserted = Column(DateTime(timezone=True), server_default=func.current_timestamp())


class APPImageCatlogue(Base):
    """
    Template for (AGRISENS Project)
    """

    __tablename__ = 'appimagecatalogue'

    id = Column(Integer)
    timestamp = Column(DateTime, primary_key=True)
    fieldname = Column(String, primary_key=True)
    fieldnumber = Column(Integer)
    user_id = Column(Integer)
    user_name = Column(String, primary_key=True)
    variable = Column(String, primary_key=True)
    value = Column(String)
    filename = Column(String)
    imagepath = Column(String) # to access the Image/Foto from the APP
    datatype_name = Column(String)
    datetime_inserted = Column(DateTime(timezone=True), server_default=func.current_timestamp())

'''
SELECT 
appobservations.aoi, 
appobservations.id, 
appobservations.timestamp, 
appobservations.fieldname, 
appobservations.fieldnumber, 
appobservations.user_id, 
appobservations.user_name, 
appobservations.lat, 
appobservations.long, 
appobservations.alt, 
appobservations.acc, 
appobservations.utc, 
appobservations.variable, 
appcode.value, 
appobservations.filename, 
appcode.label_deu, 
appcode.label_eng, 
appcode.hint_deu, 
appcode.hint_eng,
appcode.text_deu, 
appcode.text_eng, 
appcode.default_value,
appcode.type
FROM appobservations
JOIN appcode
ON(appobservations.value=appcode.value AND appobservations.variable =appcode.name)
--WHERE 
--appobservations.variable='avgPhenologicalStadium';
'''


class PlantHeight(Base):
    """
    Template for (AGRISENS Project)
    """

    __tablename__ = 'plantheight'

    id = Column(Integer, primary_key=True)
    fid = Column(String, primary_key=True)
    date = Column(DateTime, primary_key=True)
    #crop_type_code = Column(String, primary_key=True)
    crop_type = Column(String)
    variable = Column(String) # mean,  median...
    value = Column(Float)
    fid_gesamt = Column(String)
    crop_type_code = Column(String)
    sample_id = Column(Integer)
    #comment = Column(String)
    #phen_observation = Column(String)                              #entwicklungs_stadium
    #filename = Column(String, primary_key=True)
    aoi = Column(String, primary_key=True)
    geometry = Column(Geometry('POINT', #management=True,
                               srid=4326))


#        'h11', 'h12', 'h13', 'h21', 'h22', 'h23', 'h31', 'h32',
#       'h33', 'h41', 'h42', 'h43', 'FID_Gesamt', 'fr_code', 'h_median',
#       'h_mean', 'h_stdev', 'Kommentar', 'EntwStad', 'geometry'
#    'fid', 'frucht', 'fid_gesamt', 'crop_type', 'comment',
#    'phen_observation', 'geometry', 'variable', 'value', 'filename', 'aoi'

'''
class AreaOfInterest(Base):
    """
    Template for the Sina's App (AGRISENS 2 Project), DLR ...
    """

    __tablename__ = 'areaofinterest'

    #id = Column(String, primary_key=True)
    sl_nr = Column(String, primary_key=True)
    fid = Column(Integer, primary_key=True)                             # id of the field polygon
    #fid_gesamt = Column(Integer, primary_key=True)  # FID_Gesamt (FRIEN), fid (DEMM) fieldid (MRKN)
    crop_type_code = Column(String)
    #crop_type = Column(String)
    #comment = Column(String)
    year = Column(String)
    aoi = Column(String, primary_key=True)
    #filename = Column(String)
    field_geom = Column(Geometry('POLYGON', management=True, srid=4326))
'''


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
    field_geom = Column(Geometry('POLYGON', srid=4326, #management=True
                                 ))
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


class PhenObservations(Base):
    """
    Template for (AGRISENS Project)
    """

    __tablename__ = 'phenobservations'

    fid = Column(String, primary_key=True)
    date = Column(DateTime, primary_key=True)
    # crop_type_code = Column(String, primary_key=True)
    # crop_type = Column(String)
    # comment = Column(String)
    #frucht = Column(String)
    comment = Column(String)
    phen_observation = Column(String)
    bbch_code = Column(String)
    aoi = Column(String, primary_key=True)
    filename = Column(String)
    #geometry = Column(Geometry('POLYGON', management=True, srid=4326))


class UAVpattern(Base):
    """
    Template for the statistic extracted table UAV_pattern,  Template for (RCM Project)
    """

    __tablename__ = 'uavpattern'

    datetime = Column(DateTime, primary_key=True)  #2020-09-03 05:26:03
    #lat = Column(Float, primary_key=True)
    #lon = Column(Float, primary_key=True)
    pid = Column(String, primary_key=True) # 631, 537..
    orb = Column(String) # A, D
    orb_rel = Column(String) # 44
    sensor = Column(String) # S1A
    aggregation = Column(String) # 'UAV_pattern'
    spatial_ref = Column(String) # crs 4326
    pmask_date = Column(DateTime, primary_key=True) # 20210531
    statistic = Column(String, primary_key=True)  # std, mean, max, min
    value = Column(Float)
    crop_type_code = Column(String)
    polarisation = Column(String, primary_key=True) # VV, VH



