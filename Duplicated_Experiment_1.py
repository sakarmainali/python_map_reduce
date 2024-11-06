
'''
Duplicated Experiment 1: Startup Overhead: 
Here we measure how long it takes to start a MapReduce job in mrjob python. 

Input: Varied input text files
Output : Startup overhead measurement of each text files

'''


from mrjob.job import MRJob
import time
import datetime
import os
import sys

class MRWordCount(MRJob):
    def mapper(self, _, line):
        yield 'chars', len(line)
        yield 'words', len(line.split())
        yield 'lines', 1

    def reducer(self, key, values):
        yield key, sum(values)

#function to save result
def save_result(startup_overhead, input_filename):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    input_name = os.path.splitext(os.path.basename(input_filename))[0]
    filename = os.path.join("results", "Duplicated Experiment", "1", f"Duplicated_Experiment_1_Results_{input_name}_{timestamp}.txt")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, "w") as f:
        f.write("Startup overhead time: {:.4f} seconds\n".format(startup_overhead))
       


if __name__ == '__main__':

    # Get the input filename from command-line arguments for logs
    input_filename = sys.argv[-1]

    # Measure startup overhead
    start_time = time.time()

    #Run the job
    MRWordCount().run()
 
    #Calculate overhead Time
    end_time = time.time()
    startup_overhead = end_time - start_time

    # Save the performance metrics results
    save_result(startup_overhead, input_filename)