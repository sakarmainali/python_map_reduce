'''
New Experiment 1: Input Data Complexity on Performance
This experiment will evaluate how increasing the complexity and size of input data affects execution time and resource consumption. 
Input: Varied input text files in size and complexity
Output : Measurement of map reduce job time and memory usage

'''

from mrjob.job import MRJob  # Import the MRJob class from the mrjob library
import time
import psutil  # For measuring system resources
import timeit  # For measuring execution time

class MRWordFrequencyCount(MRJob):  # Define a new class that inherits from MRJob
    
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

# Function to monitor system resources like memory and CPU usage
def monitor_resources():
    memory_info = psutil.virtual_memory()
    memory_usage = memory_info.used / (1024 ** 2)  # Convert to MB
    cpu_usage = psutil.cpu_percent(interval=1)  # CPU usage in percentage
    return memory_usage, cpu_usage

if __name__ == '__main__':
    # Measure the execution time
    start_time = time.time()  # Record start time
    
    #mr_job = MRWordFrequencyCount(args=['demo_input.txt'])  # Create an instance of the MapReduce job with original tutorial
    mr_job = MRWordFrequencyCount(args=['wikipedia-dump_chunk_1.txt'])  # Create an instance of the MapReduce job with modified tutorial

    # Run the job and monitor the resources
    memory_usage_before, cpu_usage_before = monitor_resources()

    # Measure network I/O before shuffle starts
    net_io_before = psutil.net_io_counters()
    
    # Run the job
    with mr_job.make_runner() as runner:
        runner.run()
        '''
        # Collect and print the output inside the runner block
        for key, value in job.parse_output(runner.cat_output()):
            print(f'{key}: {value}')
        '''
    end_time = time.time()
    execution_time = end_time - start_time

    # Measure memory and CPU usage after the job has run
    memory_usage_after, cpu_usage_after = monitor_resources()

    # Measure network I/O after shuffle ends
    net_io_after = psutil.net_io_counters()

    cpu_usage = (cpu_usage_before + cpu_usage_after) / 2
    
    data_shuffled = (net_io_after.bytes_sent - net_io_before.bytes_sent) + (net_io_after.bytes_recv - net_io_before.bytes_recv)
    data_shuffled_mb = data_shuffled / (1024 * 1024)
    # Print the performance metrics
    print(f"Execution time: {execution_time:.4f} seconds")
    print(f"Memory usage before job: {memory_usage_before:.2f} MB")
    print(f"Memory usage after job: {memory_usage_after:.2f} MB")
    print(f"CPU usage before job: {cpu_usage_before}%")
    print(f"CPU usage after job: {cpu_usage_after}%")
    print(f"CPU Utilization: {cpu_usage}%")
    print(f"Data Shuffling Overhead: {data_shuffled_mb} MB")
