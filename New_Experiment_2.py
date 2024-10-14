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


# Compile a regular expression pattern to match words
WORD_RE = re.compile(r"[\w']+")


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
    





def monitor_resources():
    memory_info = psutil.virtual_memory()
    memory_usage = memory_info.used / (1024 ** 2)  # Convert to MB
    cpu_usage = psutil.cpu_percent(interval=1)  # CPU usage in percentage
    return memory_usage, cpu_usage

if __name__ == '__main__': 

    # Measure the execution time
    start_time = time.time()  # Record start time
    
    mr_job = MRPartitionEffectivenessExperiment()  #Get the input data as argument from commandline
    #mr_job = MRMostUsedWord()                      # For original implementation
    
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