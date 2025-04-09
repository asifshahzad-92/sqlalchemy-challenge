# Import the dependencies.
from flask import Flask, jsonify
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
import datetime as dt
import pandas as pd

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///SurfsUp/Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
Base.prepare(autoload_with=engine)

# reflect the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route('/')
def home():
    return (
        '''
        Welcome to the Hawaii Climate API! Available Routes:
        /api/v1.0/precipitation
        /api/v1.0/stations
        /api/v1.0/tobs
        /api/v1.0/<start>
        /api/v1.0/<start>/<end>
        '''
    )


# Route for precipitation data for the year period
@app.route('/api/v1.0/precipitation')
def precipitation():
    # Get the most recent date in the dataset
    most_recent_date = session.query(func.max(Measurement.date)).all()[0][0]
    most_recent_date = pd.to_datetime(most_recent_date)

    # Calculate the date a for a whole year
    one_year_ago = most_recent_date - pd.Timedelta(days=365)

    # Query the last 12 months of precipitation data
    precipitation_data = session.query(Measurement.date, Measurement.prcp) \
        .filter(Measurement.date >= one_year_ago.strftime('%Y-%m-%d')) \
        .all()

    precipitation_summary = {date: prcp for date, prcp in precipitation_data}

    return jsonify(precipitation_summary)


# Route for stations
@app.route('/api/v1.0/stations')
def stations():
    # Query all stations
    stations_data = session.query(Station.station, Station.name).all()
    stations_list = [{"station": station, "name": name} for station, name in stations_data]
    return jsonify(stations_list)

# Route for temperature observations (tobs) for the most active station in the last year
@app.route('/api/v1.0/tobs')
def tobs():
    # Get the most recent date in the dataset
    most_recent_date = session.query(func.max(Measurement.date)).all()[0][0]
    most_recent_date = pd.to_datetime(most_recent_date)
    one_year_ago = most_recent_date - pd.Timedelta(days=365)

    # Dsiaply the dates and temperature observations of the most-active station for the previous year of data.
    
    most_active_station = session.query(Measurement.station, func.count(Measurement.station)) \
        .group_by(Measurement.station) \
        .order_by(func.count(Measurement.station).desc()).first()[0]
    tobs_data = session.query(Measurement.date, Measurement.tobs) \
        .filter(Measurement.station == most_active_station) \
        .filter(Measurement.date >= one_year_ago.strftime('%Y-%m-%d')) \
        .all()

    # Convert query results to a list of dictionaries
    tobs_list = [{"date": date, "temperature": tobs} for date, tobs in tobs_data]

    return jsonify(tobs_list)


# Route for temperature statistics (min, avg, max) for a given start date
@app.route('/api/v1.0/<start>')
def start_stats(start):
    try:
        # Check if the start date exists in the dataset
        earliest_date, latest_date = session.query(
            func.min(Measurement.date),
            func.max(Measurement.date)
        ).first()

        if not (earliest_date <= start <= latest_date):
            return jsonify({
                "error": f"Date out of range. Please use a date between {earliest_date} and {latest_date}."
            }), 404

        # Calculate TMIN, TAVG, and TMAX from the start date to the end of the dataset
        stats = session.query(
            func.min(Measurement.tobs).label("TMIN"),
            func.avg(Measurement.tobs).label("TAVG"),
            func.max(Measurement.tobs).label("TMAX")
        ).filter(Measurement.date >= start).all()
        tmin, tavg, tmax = stats[0]

        # Return the statistics as a dictionary
        return jsonify({
            "Start Date": start,
            "TMIN": tmin,
            "TAVG": tavg,
            "TMAX": tmax
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


#Route for temperature statistics (min, avg, max) for a given date range (start and end)
@app.route('/api/v1.0/<start>/<end>')
def start_end_stats(start, end):
    try:
        # Check if the start and end dates exist in the dataset range
        earliest_date, latest_date = session.query(
            func.min(Measurement.date),
            func.max(Measurement.date)
        ).first()

        if not (earliest_date <= start <= latest_date):
            return jsonify({
                "error": f"Start date out of range. Please use a date between {earliest_date} and {latest_date}."
            }), 404

        if not (earliest_date <= end <= latest_date):
            return jsonify({
                "error": f"End date out of range. Please use a date between {earliest_date} and {latest_date}."
            }), 404

        if start > end:
            return jsonify({
                "error": "Start date must be earlier than or equal to the end date."
            }), 400

        # Query to calculate TMIN, TAVG, and TMAX for the date range
        stats = session.query(
            func.min(Measurement.tobs).label("TMIN"),
            func.avg(Measurement.tobs).label("TAVG"),
            func.max(Measurement.tobs).label("TMAX")
        ).filter(Measurement.date >= start).filter(Measurement.date <= end).all()
        tmin, tavg, tmax = stats[0]

        # Return the statistics as a dictionary
        return jsonify({
            "Start Date": start,
            "End Date": end,
            "TMIN": tmin,
            "TAVG": tavg,
            "TMAX": tmax
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)