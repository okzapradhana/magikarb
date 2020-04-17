import uuid
import math
import time
import datetime
import random
import csv
import os
import timeit
import json
import msgpack
import pickle
from tinydb import TinyDB
import sqlite3
import click

device_id = [ hex(val) for val in range(16, 32) ]
username = ['Andi', 'Budi', 'Taja']
lokasi = ['Bandung', 'Jakarta']
device_per_user = math.ceil(len(device_id)/len(username))

location_user = { user:lokasi[idx % len(lokasi)] 
                    for idx, user in enumerate(username) }
device_user = { user: device_id[idx*device_per_user:(idx+1)*device_per_user] 
                    for idx, user in enumerate(username)}
amount = list(range(10, 1001))

start_date = "01/01/2019"
end_date = "31/01/2019"
start_timestamp = time.mktime(
                        datetime.datetime.strptime(start_date, "%d/%m/%Y").timetuple())
end_timestamp = time.mktime(
                        datetime.datetime.strptime(end_date, "%d/%m/%Y").timetuple())

@click.command()
@click.option("--output", "-o", "output_path", default="./output/efishery.csv",
            help="Path to store generated file")
@click.option("--step", "-s", default=100000,
            help="Define amount of files to write before clear the memory")                
@click.option("--db-type", "-dt", default=None,
            help="Store generated data in specific format (sqlite or tinydb)")
def generate_file(output_path, step, db_type=None):
    '''
    Generate File in specific format.\n
    Available formats:\n
    - csv\n
    - sqlite (.db)\n
    - json\n
    - msgpack\n
    - pkl\n
    - tinydb (.json)\n
    '''
    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))

    file = open(output_path, 'w')
    if "csv" in output_path:
        writer = csv.writer(file, delimiter=',', lineterminator='\n')
        writer.writerow(['id', 'device_id', 'username', 'lokasi', 'amount', 'timestamp'])
    elif "msgpack" in output_path or "pkl" in output_path:
        file = open(output_path, "wb")
    elif db_type == "tinydb":
        db = TinyDB(output_path)
    elif db_type == "sqlite":
        conn = connection(output_path)
        create_table(conn)

    #batch process, each write 10000 data will empyting the List to clear Memory
    for timestamp in range(int(start_timestamp), int(end_timestamp+1), step):
        output = []
        #print("Current timestamp: ", timestamp)
        for curr_time_step in range(timestamp, timestamp+step):
            #print("Curr timestamp in step: ", curr_time_step)
            id_data = uuid.uuid4()
            device_id_rand = random.choice(device_id)
            username_choice = [user for user, dev_id in device_user.items()
                            if device_id_rand in dev_id][0]
            lokasi_choice = location_user[username_choice]
            amount_rand = random.choice(amount)
            if "csv" in output_path:
                output.append([id_data, device_id_rand, username_choice, lokasi_choice, amount_rand, curr_time_step])
            elif db_type == "sqlite":
                output.append((str(id_data), device_id_rand, username_choice, lokasi_choice, amount_rand, curr_time_step))
            elif "json" in output_path or "msgpack" in output_path or "pkl" in output_path:
                data = {
                    "id": str(id_data),
                    "device_id": device_id_rand,
                    "username": username_choice,
                    "lokasi": lokasi_choice,
                    "amount": amount_rand,
                    "timestamp": curr_time_step
                }
                output.append(data)
        if "csv" in output_path:
            writer.writerows(output)
        elif "json" in output_path:
            json.dump(output, file)
        elif "msgpack" in output_path:
            packed = msgpack.packb(output)
            file.write(packed)
        elif "pkl" in output_path:
            pickle.dump(output, file)
        elif db_type == "tinydb":
            db.insert_multiple(output)
        elif db_type == "sqlite":
            cursorObject = conn.cursor()
            cursorObject.executemany('''INSERT INTO efishery
            VALUES(?, ?, ?, ?, ?, ?)
            ''', output)
            conn.commit()
    file.close()

def connection(output_path):
    try:
        conn = sqlite3.connect(output_path)
        return conn
    except sqlite3.Error:
        print("Error sqlite: ", sqlite3.Error)

def create_table(conn):
    cursorObject = conn.cursor()

    cursorObject.execute('''CREATE TABLE IF NOT EXISTS efishery
    (id TEXT PRIMARY KEY NOT NULL,
    device_id CHAR(10) NOT NULL,
    username CHAR(10) NOT NULL,
    lokasi CHAR(10) NOT NULL,
    amount INT NOT NULL,
    timestamp_data TIMESTAMP NOT NULL);''')
    print("Table created successfully")

    conn.commit()

if __name__ == '__main__':
    generate_file()
    

