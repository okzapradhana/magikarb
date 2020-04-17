import timeit
import os
import solution

#In case want to compare the performance
print("Write %d files" % (solution.end_timestamp - solution.start_timestamp))
setup = "from __main__ import Solution"

files = ['efishery.csv', 'efishery.json', 'efishery.msgpack',
        'efishery.pkl', 'efisherytinydb.json', 'efishery.db']
for file in files:
    if "tinydb" in file:
        exec_time = timeit.timeit('Solution().generate_file("output/%s", 100000, db_type="tinydb")' % file, setup, number=1)
    elif "db" in file:
        exec_time = timeit.timeit('Solution().generate_file("output/%s", 100000, db_type="sqlite")' % file, setup, number=1)
    else:
        exec_time = timeit.timeit('Solution().generate_file("output/%s", 100000)' % file, setup, number=1)
print("\nExecution time when write %s format: %f seconds" % (file, exec_time))
file_size = (os.stat('output/%s' % file).st_size)/1000000
print("File size is %d MegaBytes" % file_size)