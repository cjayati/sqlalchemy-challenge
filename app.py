import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

Base = automap_base()
Base.prepare(autoload_with=engine)

Measurement = Base.classes.measurement
Station = Base.classes.station

app = Flask(__name__)

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start>n<br/>"
        f"/api/v1.0/<start>/<end><br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    """retrieve only the last 12 months of data for precipitation."""
    session = Session(engine)
    
    (most_recent,) = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    year_before_recent = dt.datetime.strptime(most_recent, '%Y-%m-%d').date() - dt.timedelta(days=365)
    # Perform a query to retrieve the data and precipitation scores
    one_year_date = session.query(Measurement.date, Measurement.prcp).\
    filter(Measurement.date >= year_before_recent).\
    filter(Measurement.date <= most_recent).\
    order_by(Measurement.date).\
    all()
    
    one_year_date_df = pd.DataFrame(one_year_date, columns=['date', 'prcp'])
    one_year_date_df.set_index('date', inplace=True)
    
    session.close()

    return jsonify(one_year_date_df.to_dict())

@app.route("/api/v1.0/stations")
def stations():
    """retrieve all station details."""
    session = Session(engine)
    
    stations = session.query(Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation).all()
    
    all_stations = []
    for station,name,latitude,longitude,elevation in stations:
        station_dict = {}
        station_dict["station"] = station
        station_dict["name"] = name
        station_dict["latitude"] = latitude
        station_dict["longitude"] = longitude
        station_dict["elevation"] = elevation
        all_stations.append(station_dict)
        
    session.close()

    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    """Query the dates and temperature observations of the most-active station for the previous year of data."""
    session = Session(engine)
    
    count_of_stations = session.query(Measurement.station, Station.name,
                                  func.count(Measurement.station)).\
    filter(Measurement.station == Station.station).\
    group_by(Measurement.station)

    active_stations = pd.DataFrame(data= count_of_stations, columns =["Station ID", "Station Name/Location", "Count of Active Stations"])

    active_stations_df= active_stations.sort_values(by= "Count of Active Stations", ascending=False)
    most_active_station = active_stations_df.iloc[0][0];
    (most_recent,) = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    year_before_recent = dt.datetime.strptime(most_recent, '%Y-%m-%d').date() - dt.timedelta(days=365)
    active_station_data = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == most_active_station).filter(Measurement.date >= year_before_recent).all()
    active_station_data_df = pd.DataFrame(active_station_data, columns=['date', 'tobs'])
    active_station_data_df.set_index('date', inplace=True)
    session.close()

    return jsonify(active_station_data_df.to_dict())


@app.route("/api/v1.0/<start>")
def maxmin_start(start):
    """Return a JSON list of the minimum temperature, the average temperature, and the maximum temperature for a specified start or start-end range."""
    session = Session(engine)
    
    (most_recent,) = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    temps = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
    filter(Measurement.date >= start).\
    filter(Measurement.date <= most_recent)
    (lowest, highest, average) = temps[0]
    session.close()
    
    temp_dict = {}
    temp_dict["minimam"] = lowest
    temp_dict["maximum"] = highest
    temp_dict["average"] = average

    return jsonify(temp_dict)

@app.route("/api/v1.0/<start>/<end>")
def maxmin_start_end(start, end):
    """Return a JSON list of the minimum temperature, the average temperature, and the maximum temperature for a specified start or start-end range."""
    session = Session(engine)
    
    temps = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
    filter(Measurement.date >= start).\
    filter(Measurement.date <= end)
    (lowest, highest, average) = temps[0]
    session.close()
    
    temp_dict = {}
    temp_dict["minimam"] = lowest
    temp_dict["maximum"] = highest
    temp_dict["average"] = average

    return jsonify(temp_dict)


if __name__ == '__main__':
    app.run(debug=True)