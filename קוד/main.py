import requests
import sqlite3
from datetime import datetime, timedelta


# create a connection to the db
conn = sqlite3.connect('data.db')
cursor = conn.cursor()


# the function clear all of the tables in the db
# input: none
# output: none
def drop_all_tables():
    # create a temporary connection to the db
    conn_delete = sqlite3.connect('data.db')
    cursor_delete = conn_delete.cursor()

    try:
        # get all the tables fro, the db
        cursor_delete.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor_delete.fetchall()

        # if their is no tables it will alert the user
        if not tables:
            print("No tables found in the database.")
            return

        # a loop that run through the tables and delete them
        for table_name in tables:
            if table_name[0] == 'sqlite_sequence':
                continue

            # delete the table
            cursor.execute(f"DROP TABLE IF EXISTS {table_name[0]};")

        conn_delete.commit()
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        # delete connection with the db
        if conn_delete:
            conn_delete.close()


# the function get the input from the api of the next 30 days
# input: none
# output: none
def get_info_api():
    current_date = datetime.now()

    # a loop that runs 5 times, each time fetching 6 days worth of data
    for _ in range(5):
        first_day = current_date.strftime('%Y-%m-%d')
        end_day = (current_date + timedelta(days=6)).strftime('%Y-%m-%d')

        try:
            # Send the request to the NASA API
            response = requests.get(
                f"https://api.nasa.gov/neo/rest/v1/feed?start_date={first_day}&end_date={end_day}&api_key=DEMO_KEY"
            )

            # Check if the response status code is 200 (OK)
            if response.status_code == 200:
                # Convert the response to JSON
                data = response.json()
                insert_to_db(data)
            else:
                # Handle HTTP errors (non-200 status code)
                print(f"Error: Received status code {response.status_code} - {response.text}")
            if response.status_code == 429:
                print("Please try to connect to another internet network")
                break
        except requests.exceptions.RequestException as e:
            # resolve problems with requests and api
            print(f"API request failed: {e}")

        except ValueError:
            # if the json cant be parsed
            print(f"Failed to parse JSON response for dates {first_day} to {end_day}")

        # Update the current date by adding 6 days for the next iteration
        current_date += timedelta(days=6)


# the function insert the data of the asteroids into the db
# input: the json response from the api
# output: none
def insert_to_db(data):
    for date, asteroids in data['near_earth_objects'].items():
        for asteroid in asteroids:
            try:
                name = asteroid['name']
                diameter = asteroid['estimated_diameter']['meters']['estimated_diameter_max']
                closest_approach_date = asteroid['close_approach_data'][0]['close_approach_date']
                relative_velocity_str = asteroid['close_approach_data'][0]['relative_velocity']['kilometers_per_hour']
                relative_velocity = float(relative_velocity_str) / 3600  # km/h to km/s

                cursor.execute(
                    "INSERT INTO Asteroids (name, diameter, closest_approach_date, relative_velocity) VALUES (?, ?, ?, ?)",
                    (name, diameter, closest_approach_date, relative_velocity)
                )
                conn.commit()  # Commit after each insertion
            except sqlite3.Error as e:
                print(f"Error inserting data into database: {e}")
            except KeyError as e:
                print(f"Missing key in asteroid data: {e}")


# the function print the five biggest asteroids
# input: none
# output: none
def send_five():
    # Get the current date
    current_date = datetime.now()
    end_date = current_date + timedelta(days=30)

    # SQL query to get the largest 5 asteroids by diameter
    cursor.execute('''
    SELECT * 
    FROM Asteroids 
    ORDER BY CAST(diameter AS REAL) DESC 
    LIMIT 5;
    ''')

    largest_asteroids = cursor.fetchall()

    # Iterate through the asteroids and print only those that are within the next 30 days
    print("Largest asteroids approaching Earth in the next 30 days:")
    for asteroid in largest_asteroids:
        # Get the closest approach date and convert it to a datetime object
        approach_date_str = asteroid[3]  # closest_approach_date is the 4th element in the tuple (index 3)

        try:
            approach_date = datetime.strptime(approach_date_str, "%Y-%m-%d")  # Assuming the date format is YYYY-MM-DD
        except ValueError:
            print(f"Invalid date format for asteroid {asteroid[1]} ({approach_date_str}), skipping.")
            continue

        # Check if the closest approach date is within the next 30 days
        if current_date <= approach_date <= end_date:
            print(f"Name: {asteroid[1]}, Diameter: {asteroid[2]} meters, "
                  f"Closest Approach Date: {asteroid[3]}, "
                  f"Relative Velocity: {asteroid[4]} km/s")
        else:
            print(f"Asteroid {asteroid[1]} will not approach Earth within the next 30 days.")
    else:
        print("No asteroids found within the next 30 days.")


def main():

    # delete tables in the db
    drop_all_tables()

    # if the db not exist it will create one
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Asteroids (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        diameter TEXT NOT NULL,
        closest_approach_date DATE NOT NULL,
        relative_velocity TEXT NOT NULL
    );
    ''')

    # call the function to get data from the api
    get_info_api()

    # print the five asteroids and save them
    send_five()

    # close access to the db
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':

    main()
