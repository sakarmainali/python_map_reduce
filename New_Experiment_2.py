'''
New Experiment 2: Custom Partitioner Effectiveness: 
Here we measure how the custom partitioner affects data distribution based on word length for most used word problem

Input: Varied input text files
Output : Measurement of custom partitioner time and memory usage

'''

from mrjob.job import MRJob  # Import the MRJob class from the mrjob library
from mrjob.step import MRStep  # Import the MRStep class for defining steps in the job
import re  # Import the regular expression module
import time
import timeit  # For measuring execution time
import psutil  # For measuring system resources
import datetime
import os
import sys

# Compile a regular expression pattern to match words
WORD_RE = re.compile(r"[\w']+")

#Original Implementation Class
class MRMostUsedWord(MRJob):  # Define a new class that inherits from MRJob

    def steps(self):  # Define the steps for the job
        return [
            MRStep(mapper=self.mapper_get_words,  # First step: map words
                   combiner=self.combiner_count_words,  # Combine word counts
                   reducer=self.reducer_count_words),  # Reduce word counts
            MRStep(reducer=self.reducer_find_max_word)  # Second step: find the max word
        ]

    def mapper_get_words(self, _, line):  # Define the mapper function
        # Yield each word in the line
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):  # Define the combiner function
        # Optimization: sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):  # Define the reducer function for counting words
        # Send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)

    def reducer_find_max_word(self, _, word_count_pairs):  # Define the reducer function for finding the max word
        # Each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(word_count_pairs)

#Modified Implementation Class
class MRPartitionEffectivenessExperiment(MRJob):  # Define a new class that inherits from MRJob

    def configure_args(self):
        """Define custom arguments such as the number of reducers."""
        super(MRPartitionEffectivenessExperiment, self).configure_args()
        self.add_passthru_arg('--num-reducers', type=int, default=5, help="Number of reducers")

    def steps(self):  # Define the steps for the job
        return [
            MRStep(mapper=self.mapper_get_words,  # First step: map words
                   combiner=self.combiner_count_words,  # Combine word counts
                   reducer=self.reducer_count_words),  # Reduce word counts
            MRStep(reducer=self.reducer_find_max_word)  # Second step: find the max word
        ]

    def mapper_get_words(self, _, line):  # Define the mapper function
        # Yield each word in the line
        for word in WORD_RE.findall(line):
            # Partition the word based on its length, to distribute the workload across reducers
            partition_id = self.get_partition(word)
            # Emit partition_id and word as key, 1 as value
            yield (partition_id, word.lower()), 1

    def combiner_count_words(self, partition_word, counts):  # Define the combiner function
        # Optimization: sum the words we've seen so far
        yield partition_word, sum(counts)

    def reducer_count_words(self, partition_word, counts):  # Define the reducer function for counting words
        # Extract partition_id and word from (partition_id, word) tuple
        partition_id, word = partition_word
        # Emit the partition ID along with the word counts for analysis
        yield partition_id, (sum(counts), word)

    def reducer_find_max_word(self, partition_id, word_count_pairs):  # Define the reducer function for finding the max word
        """
        This reducer now calculates statistics for each partition, in addition to finding the most frequent word.
        It calculates the total number of words processed in the partition and finds the most frequent word.
        """
        most_frequent_word = None
        max_count = 0
        total_words_in_partition = 0
        
        for count, word in word_count_pairs:
            total_words_in_partition += count
            if count > max_count:
                max_count = count
                most_frequent_word = word

        # Emit partition statistics
        yield f"Partition {partition_id} Stats", {
            "Total Words": total_words_in_partition,
            "Most Frequent Word": (most_frequent_word, max_count)
        }

    def get_partition(self, word):
        """
        Custom partitioning logic based on word length.
        This will ensure words of similar lengths are grouped together.
        :param word: The word to partition.
        :return: An integer representing the partition ID.
        """
        word_length = len(word)
        num_reducers = self.options.num_reducers  # Retrieve number of reducers
        # Partitioning based on word length modulo the number of reducers
        return word_length % num_reducers
    

# Function to monitor system resources
def monitor_resources():
    memory_info = psutil.virtual_memory()
    memory_usage = memory_info.used / (1024 ** 2)  # Convert to MB
    cpu_usage = psutil.cpu_percent(interval=1)  # CPU usage in percentage
    return memory_usage, cpu_usage


#function to save result
def save_result(execution_time, memory_usage_before, memory_usage_after, cpu_usage_before, cpu_usage_after, cpu_usage, data_shuffled_kb,input_filename):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    input_name = os.path.splitext(os.path.basename(input_filename))[0]
    filename = os.path.join("results", "New Experiment", "2", f"Experiment_2_results_{input_name}_{timestamp}.txt")
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

    # Measure network I/O before shuffle starts
    net_io_before = psutil.net_io_counters()

    # Monitor the resources
    memory_usage_before, cpu_usage_before = monitor_resources()
    
    # Get the input filename from command-line arguments for logs
    input_filename = sys.argv[-1]

    # Run the job
    MRPartitionEffectivenessExperiment().run()  # Create an instance of the MapReduce job with modified implementation and get the input data as argument from commandline
    #MRMostUsedWord().run()                    # Create an instance of the MapReduce job with original implementation and get the input data as argument from commandline
    
    #Calculate Time
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
