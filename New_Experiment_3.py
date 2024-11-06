'''
New Experiment 3: Combiner and Data Caching Efficiency: 
Here we measure how the combination of the combiner function and worker process caching improves performance by reducing the amount of data shuffled and the startup time for repeated jobs. 

Input: Varied input text files
Output : Measurement of custom partitioner time and memory usage

'''

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import os
import time
import timeit  
import psutil  
import datetime
import os
import sys

cols = 'Name,JobTitle,AgencyID,Agency,HireDate,AnnualSalary,GrossPay'.split(',')

class salarymax(MRJob):

    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, [ a.strip() for a in next(csv.reader([line]))]))
        
        try:
            # Yield the salary
            yield 'salary', (float(row['AnnualSalary'][1:].replace(',', '')), line)
        except ValueError:
            self.increment_counter('warn', 'missing salary', 1)
        
        # Yield the gross pay
        try:
            yield 'gross', (float(row['GrossPay'][1:].replace(',', '')), line)
        except ValueError:
            self.increment_counter('warn', 'missing gross', 1)

    def reducer(self, key, values):
        topten = []

        # For 'salary' and 'gross' compute the top 10
        for p in values:
            topten.append(p)
            topten.sort()
            topten = topten[-10:]

        for p in topten:
            yield key, p

    combiner = reducer


class CombinerAndCachingEfficiency(MRJob):

    # Add an option to control caching behavior
    def configure_args(self):
        super(CombinerAndCachingEfficiency, self).configure_args()
        self.add_passthru_arg('--use-cache', type=bool, default=False, help='Use worker caching to speed up repeated jobs')

    def mapper_init(self):
        # Initialize a cache (could be memory or local storage)
        self.cache_file = 'cached_data.txt'
        if self.options.use_cache and os.path.exists(self.cache_file):
            # Simulate loading cached data into memory
            with open(self.cache_file, 'r') as f:
                self.cache = f.readlines()
        else:
            self.cache = []

    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))

        try:
            salary = float(row['AnnualSalary'][1:].replace(',', ''))
        except ValueError:
            self.increment_counter('warn', 'missing salary', 1)
            return

        # Yield the salary and the line
        yield 'salary', (salary, line)
        
        # Cache the salary data if using caching
        if self.options.use_cache:
            self.cache.append(line)
            with open(self.cache_file, 'a') as f:
                f.write(line + '\n')

    def combiner(self, key, values):
        if key == 'salary':
            # Track the number of records processed by the combiner
            self.increment_counter('combiner', 'records_processed', sum(1 for _ in values))

            # Partial aggregation for top salaries
            top_salaries = []
            for value in values:
                top_salaries.append(value)
                top_salaries.sort(reverse=True)  # Sort in descending order
                top_salaries = top_salaries[:10]  # Keep only the top 10

            for salary in top_salaries:
                yield key, salary

    def reducer(self, key, values):
        if key == 'salary':
            # Track the number of records that reach the reducer (post-combiner)
            self.increment_counter('reducer', 'records_received', sum(1 for _ in values))

            # Final aggregation for top salaries
            top_salaries = []
            for value in values:
                top_salaries.append(value)
                top_salaries.sort(reverse=True)  # Sort in descending order
                top_salaries = top_salaries[:10]  # Keep only the top 10

            # Yield the final top 10 salaries
            for salary in top_salaries:
                yield key, salary

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]

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
    filename = os.path.join("results", "New Experiment", "3", f"Experiment_3_results_{input_name}_{timestamp}.txt")
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

     # Measure the execution time
    start_time = time.time()  # Record start time
    
    # Run the job and monitor the resources
    memory_usage_before, cpu_usage_before = monitor_resources()

    # Measure network I/O before shuffle starts
    net_io_before = psutil.net_io_counters()
    
    # Get the input filename from command-line arguments for logs
    input_filename = sys.argv[-1]

    # Run the job
    #CombinerAndCachingEfficiency().run()  # Create an instance of the MapReduce job with modified implementation
    salarymax().run()  # Create an instance of the MapReduce job with original implementation
        
    end_time = time.time()
    execution_time = end_time - start_time

    # Measure memory and CPU usage after the job has run
    memory_usage_after, cpu_usage_after = monitor_resources()

    # Measure network I/O after shuffle ends
    net_io_after = psutil.net_io_counters()

    cpu_usage = (cpu_usage_before + cpu_usage_after) / 2
    
    data_shuffled = (net_io_after.bytes_sent - net_io_before.bytes_sent) + (net_io_after.bytes_recv - net_io_before.bytes_recv)
    data_shuffled_kb = data_shuffled / (1024)

    # Save the performance metrics results
    save_result(execution_time, memory_usage_before, memory_usage_after, cpu_usage_before, cpu_usage_after, cpu_usage, data_shuffled_kb, input_filename)
