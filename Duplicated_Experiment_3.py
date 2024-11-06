'''
Duplicated Experiment 3: Sequential Scanning Speed 
Here we measure how fast the system can scan large datasets, particularly after modifications to the input file format 
Input: Varied input text files (txt) or comma separated text files
Output : Sequential Scanning speed of each text files

'''
import csv
import time
from mrjob.job import MRJob
import datetime
import os
import sys

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

#function to save result
def save_result(total_time, input_filename):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    input_name = os.path.splitext(os.path.basename(input_filename))[0]
    filename = os.path.join("results", "Duplicated Experiment", "3", f"Duplicated_Experiment_2_Results_{input_name}_{timestamp}.txt")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, "w") as f:
        f.write("Sequential scanning time : {:.4f} seconds\n".format(total_time))

if __name__ == '__main__':

    #Measure start time
    start_time = time.time()

    # Get the input filename from command-line arguments for logs
    input_filename = sys.argv[-1]

    #Run Sequential Scan job
    MRSequentialScan_txt().run()
    
    #Measure End time
    end_time = time.time()
    total_time = end_time - start_time

    #Save the performance metrics results
    save_result(total_time, input_filename)
    