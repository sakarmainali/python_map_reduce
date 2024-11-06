'''
New Experiment 1: Input Data Complexity on Performance
This experiment will evaluate how increasing the complexity and size of input data affects execution time and resource consumption. 
Input: Varied input text files in size and complexity
Output : Measurement of map reduce job time and memory usage
'''


from mrjob.job import MRJob
import time
import psutil
import datetime
import os
import sys

class MRWordFrequencyCount(MRJob):
    
    def mapper(self, _, line):  # Define the mapper function
        # Yield the total number of characters in the line
        yield "Total chars count: " ,len(line)
        # Yield the total number of words in the line
        yield "Total words count: ", len(line.split())
        # Yield the count of lines (always 1 for each line)
        yield "Total lines count: ", 1

    def reducer(self, key, values):  # Define the reducer function
        # Sum up all the values for each key and yield the result
        yield key, sum(values)

# Function to monitor system resources
def monitor_resources():
    memory_info = psutil.virtual_memory()
    memory_usage = memory_info.used / (1024 ** 2)  # Convert to MB
    cpu_usage = psutil.cpu_percent(interval=1)  # CPU usage in percentage
    return memory_usage, cpu_usage

#function to save result
def save_result(execution_time, memory_usage_before, memory_usage_after, cpu_usage_before, cpu_usage_after, cpu_usage, data_shuffled_kb, input_filename):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    input_name = os.path.splitext(os.path.basename(input_filename))[0]
    filename = os.path.join("results", "New Experiment", "1", f"New_Experiment_1_Results_{input_name}_{timestamp}.txt")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, "w") as f:
        f.write("Execution time: {:.4f} seconds\n".format(execution_time))
        f.write("Memory usage before job: {:.2f} MB\n".format(memory_usage_before))
        f.write("Memory usage after job: {:.2f} MB\n".format(memory_usage_after))
        f.write("CPU usage before job: {}%\n".format(cpu_usage_before))
        f.write("CPU usage after job: {}%\n".format(cpu_usage_after))
        f.write("Average CPU Utilization: {}%\n".format(cpu_usage))
        f.write("Data Shuffling Overhead: {:.2f} KB\n".format(data_shuffled_kb))



if __name__ == '__main__':
    start_time = time.time()

    # Monitor resources before job starts
    memory_usage_before, cpu_usage_before = monitor_resources()
    net_io_before = psutil.net_io_counters()

    # Get the input filename from command-line arguments for logs
    input_filename = sys.argv[-1]

    # Run the MRJob
    MRWordFrequencyCount().run()
    
    
    # Measure resources after job finishes
    end_time = time.time()
    execution_time = end_time - start_time
    memory_usage_after, cpu_usage_after = monitor_resources()
    net_io_after = psutil.net_io_counters()
   
    # Calculate CPU and data shuffled metrics
    cpu_usage = (cpu_usage_before + cpu_usage_after) / 2
    data_shuffled = (net_io_after.bytes_sent - net_io_before.bytes_sent) + (net_io_after.bytes_recv - net_io_before.bytes_recv)
    data_shuffled_kb = data_shuffled / 1024  # Convert to KB
    
    # Print performance metrics
    save_result(execution_time, memory_usage_before, memory_usage_after, cpu_usage_before, cpu_usage_after, cpu_usage, data_shuffled_kb, input_filename)