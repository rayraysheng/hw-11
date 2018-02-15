from flask import Flask
import jsonify

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from datetime import date, timedelta

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct
from sqlalchemy import or_

# Connect to sqlite database
engine = create_engine('sqlite:///hawaii.sqlite')

# Set up session
session = Session(engine)

app = Flask(__name__)

# Reflect tables into classes
Base = automap_base()
Base.prepare(engine, reflect=True)

# Save references to tables
Measurements = Base.classes.measurements
Stations = Base.classes.stations

@app.route('/api/v1.0/precipitation')
def date_prcp_12mths():
    prcp_12 = session.query(Measurements.date, Measurements.prcp)\
    .filter(or_(Measurements.date.like('2017-%'), # all records from 2017
            Measurements.date.like('2016-1%'), # from oct to dec of 2016
            Measurements.date.like('2016-09%'), # from sep 2016
            Measurements.date.like('2016-08-3%'), # from aug 30 and 31 2016
            Measurements.date.like('2016-08-24'), # the rest added manually
            Measurements.date.like('2016-08-25'),
            Measurements.date.like('2016-08-26'),
            Measurements.date.like('2016-08-27'),
            Measurements.date.like('2016-08-28'),
            Measurements.date.like('2016-08-29'))
       )
    
    date_list = []
    prcp_list = []

    for each in prcp_12.all():
    
        date_list.append(each[0])
        prcp_list.append(each[1])

    prcp_dict = {'date': date_list, 'prcp': prcp_list}
    
    return jsonify(prcp_dict)

@app.route('/api/v1.0/stations')
def show_stations():
    stations_list = [each[0] for each in session.query(Stations.station).all()]
    
    return jsonify(stations_list)

@app.route('/api/v1.0/tobs')
def date_tobs_12mths():
    tobs_12mths = session.query(Measurements.tobs)\
    .filter(or_(Measurements.date.like('2017-%'), # all records from 2017
            Measurements.date.like('2016-1%'), # from oct to dec of 2016
            Measurements.date.like('2016-09%'), # from sep 2016
            Measurements.date.like('2016-08-3%'), # from aug 30 and 31 2016
            Measurements.date.like('2016-08-24'), # the rest added manually
            Measurements.date.like('2016-08-25'),
            Measurements.date.like('2016-08-26'),
            Measurements.date.like('2016-08-27'),
            Measurements.date.like('2016-08-28'),
            Measurements.date.like('2016-08-29'))
       )

    tobs_list = [each[0] for each in tobs_12mths]
    
    return jsonify(tobs_list)

# For the next 2 parts, use functions
# Step 1: parse date strings to date object
def parse_date(date_string):
    try:
        date_parts = date_string.split('-')

        year = int(date_parts[0])
        month = int(date_parts[1])
        day = int(date_parts[2])

        date_object = date(year, month, day)
        return date_object
    except:
        print('Error: Check date string format')
        
# Step 2: get all the dates between the starting and ending dates
def get_dates_between(start_date_obj, end_date_obj):
    
    date_obj_list = []
    delta = end_date_obj - start_date_obj
    
    for i in range(delta.days+1):
        new_date = start_date_obj + timedelta(days=i)
        date_obj_list.append(new_date)
        
    return date_obj_list

# Step 3: reformat the in-between dates to string
def convert_dates_list(date_obj_list):
    date_str_list = [date.strftime(each, '%Y-%m-%d')\
                         for each in date_obj_list]
    return date_str_list

# Step 4: query data from the target dates
def query_temps(date_str_list):
    temp_stats_query = session.query(Measurements.date, 
                               func.min(Measurements.tobs), 
                               func.avg(Measurements.tobs),
                               func.max(Measurements.tobs))\
    .filter(Measurements.date.in_(date_str_list)).group_by(Measurements.date)
    
    return temp_stats_query

# Step 5: use the query results to make a DataFrame
def make_temp_dict_list(temp_stats_query):
    temp_stats_list = []

    for each in temp_stats_query.all():

        temp_stats_dict = {
            'date': each[0],
            'low': each[1],
            'avg': each[2],
            'high': each[3]
        }

        temp_stats_list.append(temp_stats_dict)
    
    return temp_stats_list

# Putting it all together
def start_to_end(start_date_str, end_date_str):
    
    start_date_obj = parse_date(start_date_str)
    end_date_obj = parse_date(end_date_str)
    
    date_obj_list = get_dates_between(start_date_obj, end_date_obj)
    
    date_str_list = convert_dates_list(date_obj_list)
    
    temp_stats_query = query_temps(date_str_list)
    
    temp_stats_list = make_temp_dict_list(temp_stats_query)
    
    return temp_stats_list

@app.route('/api/v1.0/<start>')
def date_temps_start():
    start_date = start
    end_date = '2017-08-23'
    
    temp_stats_list = start_to_end(start_date, end_date)
    
    return jsonify(temp_stats_list)

@app.route('/api/v1.0/<start>/<end>')
def date_temps_range():
    start_date = start
    end_date = end
    
    temp_stats_list = start_to_end(start_date, end_date)
    
    return jsonify(temp_stats_list)

if __name__ == '__main__':
    app.run(debug=True, port=5009)