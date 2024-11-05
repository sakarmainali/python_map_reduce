'''
Duplicated Experiment 3: Sequential Scanning Speed 
Here we measure how fast the system can scan large datasets, particularly after modifications to the input file format 
Input: Varied input text files (txt) or comma separated text files
Output : Sequential Scanning speed of each text files

'''
import csv
import time
from mrjob.job import MRJob

class MRSequentialScan_csv(MRJob):
    def mapper(self, _, line):
        row = next(csv.reader([line]))
        yield 'chars', len(line)
        yield 'fields', len(row)

    def reducer(self, key, values):
        yield key, sum(values)

class MRSequentialScan_txt(MRJob):
    def mapper(self, _, line):
        yield 'chars', len(line)
        yield 'fields', 1  # Each line is considered as one field

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    start_time = time.time()
    #Input file
    input_file = 'Tutorial_1_2_Input_1.txt'

    #Sequential Scan job
    mr_job = MRSequentialScan_txt(args=[input_file])
    
    with mr_job.make_runner() as runner:
        runner.run()
    
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Sequential scanning time : {total_time} seconds")