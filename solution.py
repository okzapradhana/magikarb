import uuid
import math
import time
import datetime
import random
import csv
import os
import timeit

class Solution(object):
    def __init__(self):
        self.device_id = [ hex(val) for val in range(16, 32) ]
        self.username = ['Andi', 'Budi', 'Taja']
        self.lokasi = ['Bandung', 'Jakarta']
        self.device_per_user = math.ceil(len(self.device_id)/len(self.username))
        
        self.location_user = { user:self.lokasi[idx % len(self.lokasi)] 
                            for idx, user in enumerate(self.username) }
        self.device_user = { user: self.device_id[idx*self.device_per_user:(idx+1)*self.device_per_user] 
                            for idx, user in enumerate(self.username)}
        self.amount = list(range(10, 1001))
        
        self.start_date = "01/01/2019"
        self.end_date = "03/01/2019"
        self.start_timestamp = time.mktime(
                                datetime.datetime.strptime(self.start_date, "%d/%m/%Y").timetuple())
        self.end_timestamp = time.mktime(
                                datetime.datetime.strptime(self.end_date, "%d/%m/%Y").timetuple())
        print(self.start_timestamp)
        print(self.end_timestamp)

    def generate_file(self, output_path, step):
        if not os.path.exists(os.path.dirname(output_path)):
            os.makedirs(os.path.dirname(output_path))

        file = open(output_path, 'w')
        writer = csv.writer(file, delimiter=',', lineterminator='\n')
        writer.writerow(['id', 'device_id', 'username', 'lokasi', 'amount', 'timestamp'])
        
        #batch process, each write 10000 data will empyting the List to clear Memory
        for timestamp in range(int(self.start_timestamp), int(self.end_timestamp+1), step):
            output = []
            print("Current timestamp: ", timestamp)
            for curr_time_step in range(timestamp, timestamp+step):
                print("Curr timestamp in step: ", curr_time_step)
                id = uuid.uuid4()
                device_id_rand = random.choice(self.device_id)
                username_choice = [user for user, dev_id in self.device_user.items()
                                if device_id_rand in dev_id][0]
                lokasi_choice = self.location_user[username_choice]
                amount_rand = random.choice(self.amount)
                output.append([id, device_id_rand, username_choice, lokasi_choice, amount_rand, curr_time_step])
            writer.writerows(output)

if __name__ == '__main__':
    setup = "from __main__ import Solution"
    exec_time = timeit.timeit('Solution().generate_file(output_path="output/efishery.csv", step=100000)', setup, number=1)
    print("Execution time when write as csv format: ", exec_time)
