import re
import numpy as np
import pandas as pd
from geopy.distance import geodesic


# Helper function to convert DMS to decimal degrees
def dms_to_dd(dms):
    parts = dms.split("'")
    degrees = float(parts[0])
    minutes = float(parts[1])
    seconds = float(parts[2])
    return degrees + (minutes / 60) + (seconds / 3600)


# Function to calculate distance between two points
def calculate_distance(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).meters


# Function to calculate bearing between two points
def calculate_bearing(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    x = np.sin(dlon) * np.cos(lat2)
    y = np.cos(lat1) * np.sin(lat2) - (np.sin(lat1) * np.cos(lat2) * np.cos(dlon))
    initial_bearing = np.arctan2(x, y)
    initial_bearing = np.degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360
    return compass_bearing


# Function to categorize aspect
def categorise_aspect(numerical_aspect):

    match numerical_aspect:
        case aspect if aspect <= 45 or aspect >= 315:
            return "hot"
        case aspect if 135 <= aspect <= 225:
            return "cold"
        case aspect if 45 < aspect < 135 or 225 < aspect < 315:
            return "beaming"
        case _:
            return "unknown"


# Function to determine the correct word to say for altitude difference
def categorise_altitude_differential(from_altitude, to_altitude):

    differential = from_altitude - to_altitude

    match differential:
        case diff if diff > 1000:
            return "low"
        case diff if diff < -1000:
            return "high"
        case _:
            return "co altitude"


# Function to clean callsign by removing non-alphanumeric characters and converting to lowercase
def clean_callsign(callsign):
    return re.sub(r'\W+', '', callsign).lower()


# Function to process the input string and find the closest red coalition target
def process_input(input_string, input_df, blue_callsigns_tripwire):
    parts = input_string.split()
    if parts[0].lower() == "overlord":
        callsign = clean_callsign(parts[1])
        filtered_df = input_df[(input_df['from_clean'] == callsign) & (input_df['coalition_to'] == "red")]

        # Insert all the other potential instructions to overlord here with if else, may change to match case for
        # readability in the future.
        command = ' '.join(parts[2:]).lower()

        if "bogey dope" in command:
            return bogey_dope(filtered_df)
        elif "tripwire" in command:
            try:
                tripwire_distance = float(parts[-1])
                blue_callsigns_tripwire.append({'callsign': callsign, 'tripwire_distance': tripwire_distance})
                return
            except ValueError:
                return "Invalid tripwire distance."
        else:
            return "Invalid input."
    else:
        return "Invalid input."


# Bogey Dope function
def bogey_dope(filtered_df):
    if not filtered_df.empty:
        closest_row = filtered_df.loc[filtered_df['distance'].idxmin()]
        return closest_row
    else:
        return "No red coalition targets found."


# Function to compute metrics on the fly for blue to blue pairs
def compute_blue_to_blue_metrics(callsign, df_blue):
    row_from = df_blue[df_blue['callsign'] == callsign].iloc[0]
    results = []
    for _, row_to in df_blue[df_blue['callsign'] != callsign].iterrows():
        distance = calculate_distance(row_from['northing_dd'], row_from['easting_dd'], row_to['northing_dd'], row_to['easting_dd'])
        bearing = calculate_bearing(row_from['northing_dd'], row_from['easting_dd'], row_to['northing_dd'], row_to['easting_dd'])
        numerical_aspect = abs(bearing - row_from['heading']) % 360
        aspect = categorise_aspect(numerical_aspect)
        results.append({
            'from': row_from['callsign'],
            'to': row_to['callsign'],
            'distance': distance,
            'bearing': bearing,
            'numerical_aspect': numerical_aspect,
            'aspect': aspect
        })
    return pd.DataFrame(results)


# Tripwire functionality
def check_tripwire(input_df, blue_callsigns_tripwire):
    results = []
    for entry in blue_callsigns_tripwire:
        blue_callsign = entry['callsign']
        tripwire_distance = entry['tripwire_distance']

        # Filter the DataFrame for the given blue callsign and tripwire distance
        filtered_df = input_df[(input_df['from_clean'] == blue_callsign) &
                               (input_df['coalition_to'] == "red") &
                               (input_df['distance'] <= tripwire_distance)]

        if not filtered_df.empty:
            results.append({
                'callsign': blue_callsign,
                'targets': filtered_df.to_dict(orient='records')
            })
        else:
            results.append({
                'callsign': blue_callsign,
                'targets': "No red coalition targets within tripwire distance."
            })

    return results
