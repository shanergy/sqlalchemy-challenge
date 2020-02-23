# ************************************************** #
# Import Dependencies
# ************************************************** #
import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify

# ************************************************** #
# Database Setup
# ************************************************** #
# Set database filepath
hawaii_db_filepath = "../../SMU_DS/02-Homework/10-Advanced-Data-Storage-and-Retrieval/Instructions/Resources/hawaii.sqlite"

# Create an engine that uses and and communicates with the "hawaii.sqlite" database
# engine = create_engine("sqlite:///Resources/hawaii.sqlite")
engine = create_engine(f"sqlite:///{hawaii_db_filepath}")

# Declare Base using automap_base()
Base = automap_base()

# Use Base class to reflect the database tables
Base.prepare(engine, reflect=True)

# Set a reference for each class/table
Station = Base.classes.station
Measurement = Base.classes.measurement

# ************************************************** #
# Flask Setup (Create an App)
# ************************************************** #
app = Flask(__name__)

# ************************************************** #
# Flask Routes (Define Static Routes)
# ************************************************** #
########################################################################################################################
@app.route("/")
def home():
    print("The server has received a request for 'Home' page.")
    return(f"Welcome to the Climate App Home page!<br>"
           f"<br>"
           f"--------------------<br>"
           f"Available Routes:<br>"
           f"--------------------<br>"
           f"/api/v1.0/precipitation<br>"
           f"/api/v1.0/stations<br>"
           f"/api/v1.0/tobs<br>"
           f"/api/v1.0/start_date<br>"
           f"/api/v1.0/start_date/end_date<br>"
           f"--------------------<br>"
          )
########################################################################################################################
@app.route("/api/v1.0/precipitation")
def precipitation():
    print("The server has received a request for 'precipitation' page.")
    
    # Create session from Python to the database
    session = Session(engine)
    
    # Calculate 1 year ago from latest/max date using engine.execute and DATE(date,'-1 year'), unpack from returned list & tuple
    measurement_max_date_minus_1_year = engine.execute("SELECT DATE(MAX(date),'-1 year') FROM measurement").fetchall()[0][0]

    # Query last 12 months of prcp values, averaged across all stations and grouped by date
    last_12_months_avg_prcp_by_day = session.query(Measurement.date, func.avg(Measurement.prcp)).\
                                                filter(Measurement.date >= measurement_max_date_minus_1_year).\
                                                group_by(Measurement.date).\
                                                order_by(Measurement.date).all()
    
    # Close the session
    session.close()

    # Convert the query results to a Dictionary using date as the key and tobs as the value.
    last_12_months_avg_prcp_by_day = dict(last_12_months_avg_prcp_by_day)
    
    # Return the JSON representation of your dictionary.
    return jsonify(last_12_months_avg_prcp_by_day)
########################################################################################################################
@app.route("/api/v1.0/stations")
def stations():
    print("The server has received a request for 'stations' page.")
    
    # Create session from Python to the database
    session = Session(engine)
    
    # Save the query (station, grouped by station) results
    stations = session.query(Measurement.station).group_by(Measurement.station).all()
    
    # Close the session
    session.close()
    
    # Create a list of the stations
    station_list = []
    for i in range(len(stations)):
        station_list.append(stations[i][0])
    
    # Return a JSON list of stations from the dataset
    return jsonify(station_list)
########################################################################################################################
@app.route("/api/v1.0/tobs")
def tobs():
    print("The server has received a request for 'tobs' page.")
    
    # Create session from Python to the database
    session = Session(engine)
    
    # Calculate 1 year ago from latest/max date using engine.execute and DATE(date,'-1 year'), unpack from returned list & tuple
    measurement_max_date_minus_1_year = engine.execute("SELECT DATE(MAX(date),'-1 year') FROM measurement").fetchall()[0][0]

    # Identify station with the highest number of observations
    station_with_most_obs = session.query(Measurement.station, func.count(Measurement.station)).\
                                    group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()
    
    # Query last 12 months of tobs values from the station with the most observations
    last_12_months_tobs_by_day = session.query(Measurement.date, Measurement.tobs).\
                                            filter(Measurement.date >= measurement_max_date_minus_1_year).\
                                            filter(Measurement.station == station_with_most_obs[0]).\
                                            order_by(Measurement.date).all()
    
    # Close the session
    session.close()

    # Convert the query results to a Dictionary using date as the key and tobs as the value.
    last_12_months_tobs_by_day = dict(last_12_months_tobs_by_day)
    
    # Create list
    last_12_months_tobs_by_day_list = [last_12_months_tobs_by_day]
    
    # Return a JSON list of Temperature Observations (tobs) for the previous year.
    return jsonify(last_12_months_tobs_by_day_list)
########################################################################################################################
# ************************************************** #
# Flask Routes (Define Dynamic Routes)
# ************************************************** #
@app.route("/api/v1.0/<start>")
def start(start):
    print("The server has received a request for 'start_end' page.")
    
    """TMIN, TAVG, and TMAX for a list of dates.
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
    Returns:
        TMIN, TAVE, and TMAX
    """
    
    # Define start_date as the date passed through from the route/url
    start_date = start
    
    # Create session from Python to the database
    session = Session(engine)
    
    # Query greater than and equal to the start date calculating min, max and average tobs/temps
    start_query_results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                                    filter(Measurement.date >= start_date).all()
    
    # Create empty list
    start_query_results_list = []

    # Loop through returned tuple in list, unpacking values and placing in a new list
    for i in range(len(start_query_results[0])):
        start_query_results_list.append(start_query_results[0][i])
    
    # Close the session
    session.close()
    
    # Return a JSON list of the minimum temperature, the avg temperature, and the max temperature for a given start range.
    return jsonify(start_query_results_list)

@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
    print("The server has received a request for 'start_end' page.")
    
    """TMIN, TAVG, and TMAX for a list of dates.
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
    Returns:
        TMIN, TAVE, and TMAX
    """
    
    # Define start_date and end_date as the dates passed through from the route/url
    start_date = start
    end_date = end
    
    # Create session from Python to the database
    session = Session(engine)
    
    # Query between start and end dates calculating min, max and average tobs/temps
    start_end_query_results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                                        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    
    # Create empty list
    start_end_query_results_list = []

    # Loop through returned tuple in list, unpacking values and placing in a new list
    for i in range(len(start_end_query_results[0])):
        start_end_query_results_list.append(start_end_query_results[0][i])
    
    # Close the session
    session.close()
    
    # Return a JSON list of the minimum temperature, the avg temperature, and the max temperature for a given start-end range.    
    return jsonify(start_end_query_results_list)

# ************************************************** #
# Define main Behavior
# ************************************************** #
if __name__ == "__main__":
    app.run(debug=True)