'''
Duplicated Experiment 2: Data Shuffling Overhead :
Here we will measure time and resources spent shuffling data between maps and reduce tasks, particularly with the new combiner function.  

Input: Varied input text files with combiner and without combiner
Output : Data Shuffling Overhead of map reduce job

'''
import time
from mrjob.job import MRJob
import psutil  # To measure memory usage (optional)
import datetime
import os
import sys


class MRWordCountWithCombiner(MRJob):
    def mapper(self, _, line):
        yield 'words', len(line.split())

    def combiner(self, key, values):
        # Combine values before sending to reducer
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

class MRWordCountWithoutCombiner(MRJob):
    def mapper(self, _, line):
        yield 'words', len(line.split())

    def reducer(self, key, values):
        yield key, sum(values)

# Function to monitor system resources
def measure_memory():
    # Function to measure memory usage
    process = psutil.Process()
    return process.memory_info().rss / (1024 * 1024)  # Return memory usage in MB

#function to save result
def save_result(total_time,total_memory,data_shuffled_kb, input_filename):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    input_name = os.path.splitext(os.path.basename(input_filename))[0]
    filename = os.path.join("results", "Duplicated Experiment", "2", f"Duplicated_Experiment_2_Results_{input_name}_{timestamp}.txt")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, "w") as f:
        f.write("Total job execution time: {:.4f} seconds\n".format(total_time))
        f.write("Memory usage: {:.4f} seconds\n".format(total_memory))
        f.write("Data Shuffling Overhead: {:.4f} seconds\n".format(data_shuffled_kb))

if __name__ == '__main__':

    # Get the input filename from command-line arguments for logs
    input_filename = sys.argv[-1]


    # Measure performance with combiner
    start_time = time.time()
    start_memory = measure_memory()

    # Measure network I/O before shuffle starts
    net_io_before = psutil.net_io_counters()

    #Run the mrjob with combiner
    MRWordCountWithCombiner().run()
    #MRWordCountWithoutCombiner().run()  #Original implementation

    
    end_memory = measure_memory()
    end_time = time.time()
    # Measure network I/O after shuffle ends
    net_io_after = psutil.net_io_counters()

    total_time = end_time - start_time
    total_memory = end_memory - start_memory
    data_shuffled = (net_io_after.bytes_sent - net_io_before.bytes_sent) + (net_io_after.bytes_recv - net_io_before.bytes_recv)
    data_shuffled_kb = data_shuffled / (1024)
    
    #Save the performance metrics results
    save_result(total_time,total_memory,data_shuffled_kb, input_filename)

