import pandas as pd
import time

from helper_functions import dms_to_dd, calculate_distance, calculate_bearing, categorise_aspect, \
    categorise_altitude_differential, clean_callsign, process_input, check_tripwire

# Record the start time
start_time = time.time()

# Define the data as a list of dictionaries
data = [
    {"coalition": "blue", "type": "A-20", "callsign": "Bravo 1-1", "northing": "34'30'23.5102", "easting": "43'53'44.7988", "altitude": 3988, "heading": 178},
    {"coalition": "blue", "type": "F-16", "callsign": "Stingray 1-1", "northing": "35'30'23.5102", "easting": "42'53'44.7988", "altitude": 20000, "heading": 292},
    {"coalition": "blue", "type": "F-16", "callsign": "Stingray 1-2", "northing": "35'30'23.5102", "easting": "42'53'43.7988", "altitude": 20000, "heading": 292},
    {"coalition": "red", "type": "MiG-29", "callsign": "REDFOR 1-1", "northing": "30'30'23.5102", "easting": "40'53'44.7988", "altitude": 10000, "heading": 320},
    {"coalition": "red", "type": "MiG-29", "callsign": "REDFOR 1-2", "northing": "38'30'23.5102", "easting": "42'53'44.7988", "altitude": 10000, "heading": 90},
    {"coalition": "red", "type": "Su-27", "callsign": "REDFOR 2-1", "northing": "37'20'43.2232", "easting": "39'33'66.1988", "altitude": 19000, "heading": 359},
    {"coalition": "red", "type": "Su-27", "callsign": "REDFOR 2-2", "northing": "37'20'43.2232", "easting": "39'33'69.1988", "altitude": 22000, "heading": 359},
    {"coalition": "blue", "type": "F-15", "callsign": "Angel 1-1", "northing": "38'10'23.5102", "easting": "42'10'44.1242", "altitude": 30000, "heading": 180},
    {"coalition": "blue", "type": "F-15", "callsign": "Angel 1-2", "northing": "38'10'23.5102", "easting": "42'10'43.1246", "altitude": 30000, "heading": 180},
    {"coalition": "blue", "type": "F-14", "callsign": "Chevy 1-1", "northing": "35'10'22.3241", "easting": "39'10'43.1242", "altitude": 24000, "heading": 100},
    {"coalition": "blue", "type": "F-14", "callsign": "Chevy 1-2", "northing": "35'10'23.7634", "easting": "39'10'43.1246", "altitude": 26000, "heading": 100},
    {"coalition": "blue", "type": "F/A-18", "callsign": "Hornet 1-1", "northing": "36'20'23.5102", "easting": "41'53'44.7988", "altitude": 15000, "heading": 270},
    {"coalition": "blue", "type": "F/A-18", "callsign": "Hornet 1-2", "northing": "36'20'24.5102", "easting": "41'53'45.7988", "altitude": 15000, "heading": 270},
    {"coalition": "red", "type": "Su-30", "callsign": "REDFOR 3-1", "northing": "32'30'23.5102", "easting": "38'53'44.7988", "altitude": 18000, "heading": 310},
    {"coalition": "red", "type": "Su-30", "callsign": "REDFOR 3-2", "northing": "32'30'24.5102", "easting": "38'53'45.7988", "altitude": 18000, "heading": 310},
    {"coalition": "blue", "type": "AV-8B", "callsign": "Harrier 1-1", "northing": "37'40'23.5102", "easting": "40'53'44.7988", "altitude": 25000, "heading": 290},
    {"coalition": "blue", "type": "AV-8B", "callsign": "Harrier 1-2", "northing": "37'40'24.5102", "easting": "40'53'45.7988", "altitude": 25000, "heading": 290},
    {"coalition": "red", "type": "J-11", "callsign": "REDFOR 4-1", "northing": "33'30'23.5102", "easting": "39'53'44.7988", "altitude": 22000, "heading": 340},
    {"coalition": "red", "type": "J-11", "callsign": "REDFOR 4-2", "northing": "33'30'24.5102", "easting": "39'53'45.7988", "altitude": 22000, "heading": 340},
    {"coalition": "blue", "type": "C-130", "callsign": "Hercules 1-1", "northing": "34'50'23.5102", "easting": "43'30'44.7988", "altitude": 10000, "heading": 180},
    {"coalition": "red", "type": "A-50", "callsign": "Mainstay 1-1", "northing": "32'10'23.5102", "easting": "40'10'44.7988", "altitude": 25000, "heading": 270},
    {"coalition": "blue", "type": "E-3", "callsign": "Sentry 1-1", "northing": "36'30'23.5102", "easting": "41'30'44.7988", "altitude": 30000, "heading": 90},
    {"coalition": "blue", "type": "E-2", "callsign": "Hawkeye 1-1", "northing": "35'50'23.5102", "easting": "42'50'44.7988", "altitude": 28000, "heading": 360},
    {"coalition": "blue", "type": "F-4", "callsign": "Phantom 1-1", "northing": "36'10'23.5102", "easting": "41'10'44.7988", "altitude": 22000, "heading": 270},
    {"coalition": "blue", "type": "F-4", "callsign": "Phantom 1-2", "northing": "36'10'24.5102", "easting": "41'10'45.7988", "altitude": 22000, "heading": 270},
    {"coalition": "blue", "type": "F-5", "callsign": "Tiger 1-1", "northing": "35'40'23.5102", "easting": "40'40'44.7988", "altitude": 18000, "heading": 290},
    {"coalition": "blue", "type": "F-5", "callsign": "Tiger 1-2", "northing": "35'40'24.5102", "easting": "40'40'45.7988", "altitude": 18000, "heading": 290},
    {"coalition": "red", "type": "Mirage F1", "callsign": "REDFOR 5-1", "northing": "33'50'23.5102", "easting": "38'50'44.7988", "altitude": 21000, "heading": 320},
    {"coalition": "red", "type": "Mirage F1", "callsign": "REDFOR 5-2", "northing": "33'50'24.5102", "easting": "38'50'45.7988", "altitude": 21000, "heading": 320},
    {"coalition": "red", "type": "MiG-21", "callsign": "REDFOR 6-1", "northing": "31'40'23.5102", "easting": "39'40'44.7988", "altitude": 15000, "heading": 270},
    {"coalition": "red", "type": "MiG-21", "callsign": "REDFOR 6-2", "northing": "31'40'24.5102", "easting": "39'40'45.7988", "altitude": 15000, "heading": 270},
    {"coalition": "red", "type": "MiG-23", "callsign": "REDFOR 7-1", "northing": "32'50'23.5102", "easting": "40'50'44.7988", "altitude": 18000, "heading": 290},
    {"coalition": "red", "type": "MiG-23", "callsign": "REDFOR 7-2", "northing": "32'50'24.5102", "easting": "40'50'45.7988", "altitude": 18000, "heading": 290},
    {"coalition": "red", "type": "MiG-19", "callsign": "REDFOR 8-1", "northing": "33'60'23.5102", "easting": "41'60'44.7988", "altitude": 12000, "heading": 310},
    {"coalition": "red", "type": "MiG-19", "callsign": "REDFOR 8-2", "northing": "33'60'24.5102", "easting": "41'60'45.7988", "altitude": 12000, "heading": 310},
    {"coalition": "blue", "type": "F-16", "callsign": "Ranger 1-1", "northing": "35'40'23.5102", "easting": "42'40'44.7988", "altitude": 21000, "heading": 270},
    {"coalition": "blue", "type": "F-16", "callsign": "Ranger 1-2", "northing": "35'40'24.5102", "easting": "42'40'45.7988", "altitude": 21000, "heading": 270},
    {"coalition": "blue", "type": "F-16", "callsign": "Ranger 1-3", "northing": "35'40'25.5102", "easting": "42'40'46.7988", "altitude": 21000, "heading": 270},
    {"coalition": "blue", "type": "F-16", "callsign": "Ranger 1-4", "northing": "35'40'26.5102", "easting": "42'40'47.7988", "altitude": 21000, "heading": 270},
    {"coalition": "blue", "type": "F-16", "callsign": "Thunder 1-1", "northing": "36'10'23.5102", "easting": "41'50'44.7988", "altitude": 22000, "heading": 280},
    {"coalition": "blue", "type": "F-16", "callsign": "Thunder 1-2", "northing": "36'10'24.5102", "easting": "41'50'45.7988", "altitude": 22000, "heading": 280},
    {"coalition": "blue", "type": "F-16", "callsign": "Thunder 1-3", "northing": "36'10'25.5102", "easting": "41'50'46.7988", "altitude": 22000, "heading": 280},
    {"coalition": "blue", "type": "F-16", "callsign": "Thunder 1-4", "northing": "36'10'26.5102", "easting": "41'50'47.7988", "altitude": 22000, "heading": 280},
    {"coalition": "red", "type": "MiG-21", "callsign": "REDFOR 6-1", "northing": "31'40'23.5102", "easting": "39'40'44.7988", "altitude": 15000, "heading": 270},
    {"coalition": "red", "type": "MiG-21", "callsign": "REDFOR 6-2", "northing": "31'40'24.5102", "easting": "39'40'45.7988", "altitude": 15000, "heading": 270},
    {"coalition": "red", "type": "MiG-21", "callsign": "REDFOR 7-1", "northing": "32'50'23.5102", "easting": "40'50'44.7988", "altitude": 18000, "heading": 290},
    {"coalition": "red", "type": "MiG-21", "callsign": "REDFOR 7-2", "northing": "32'50'24.5102", "easting": "40'50'45.7988", "altitude": 18000, "heading": 290}
]

# Create the DataFrame
df = pd.DataFrame(data)

# Convert northing and easting to decimal degrees
df['northing_dd'] = df['northing'].apply(dms_to_dd)
df['easting_dd'] = df['easting'].apply(dms_to_dd)

df_blue = df[df['coalition'] == 'blue']
df_red = df[df['coalition'] == 'red']

# TODO filter blue df based on who has checked in - implement function for updating that list somewhere else

# Create a Cartesian product of the blue and red DataFrames
df_cartesian = pd.merge(df_blue.assign(key=1), df_red.assign(key=1), on='key', suffixes=('_from', '_to'))

# Calculate distances and bearings using cartesian product and .apply, kinda like scala map
df_cartesian['distance'] = df_cartesian\
    .apply(lambda row: calculate_distance(row['northing_dd_from'],
                                          row['easting_dd_from'],
                                          row['northing_dd_to'],
                                          row['easting_dd_to']),
           axis=1)
df_cartesian['bearing'] = df_cartesian\
    .apply(lambda row: calculate_bearing(row['northing_dd_from'],
                                         row['easting_dd_from'],
                                         row['northing_dd_to'],
                                         row['easting_dd_to']),
           axis=1)
df_cartesian['altitude_differential'] = df_cartesian\
    .apply(lambda row: categorise_altitude_differential(row['altitude_from'],
                                                        row['altitude_to']),
           axis=1)


# Calculate numerical aspect and categorize it
df_cartesian['numerical_aspect'] = (df_cartesian['bearing'] - df_cartesian['heading_to']).abs() % 360
df_cartesian['aspect'] = df_cartesian['numerical_aspect'].apply(categorise_aspect)

# Add a cleaned version of the 'from' column to the results DataFrame
df_cartesian['from_clean'] = df_cartesian['callsign_from'].apply(clean_callsign)

# Select only the necessary columns
columns_to_keep = [
    'coalition_from', 'type_from', 'callsign_from', 'altitude_from', 'heading_from', 'coalition_to', 'type_to',
    'callsign_to', 'altitude_to', 'heading_to', 'distance', 'bearing', 'altitude_differential', 'numerical_aspect',
    'aspect', 'from_clean'
]
df_selected = df_cartesian[columns_to_keep]


# Set pandas display options to show all columns and a wide display width
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Display the results DataFrame
print(df_selected)
print('\n')

blue_tripwire = []

# Example usage bogey dope
voice_string_1 = "overlord thunder14 bogey dope"
thunder_14_bogey_dope = process_input(voice_string_1, df_selected, blue_tripwire)
print(thunder_14_bogey_dope)
print('\n')

# Example usage bogey dope
voice_string_2 = "overlord phantom11 bogey dope"
phantom_11_bogey_dope = process_input(voice_string_2, df_selected, blue_tripwire)
print(phantom_11_bogey_dope)
print('\n')

# Example usage bogey dope
voice_string_3 = "overlord chevy12 bogey dope"
chevy_11_bogey_dope = process_input(voice_string_3, df_selected, blue_tripwire)
print(chevy_11_bogey_dope)
print('\n')

# Example usage bogey dope
voice_string_4 = "overlord angel11 bogey dope"
angel_11_bogey_dope = process_input(voice_string_4, df_selected, blue_tripwire)
print(angel_11_bogey_dope)
print('\n')

# Example usage set tripwire
voice_string_5 = "overlord ranger12 set tripwire 200000"
ranger_12_tripwire = process_input(voice_string_5, df_selected, blue_tripwire)
print(blue_tripwire)
print('\n')

# Example usage tripwire check
tripwire_check = check_tripwire(df_selected, blue_tripwire)
print(tripwire_check)
print('\n')

# Record the end time
end_time = time.time()

print('\n')

# Calculate the elapsed time
elapsed_time = end_time - start_time
print('Execution time:', elapsed_time, 'seconds')
