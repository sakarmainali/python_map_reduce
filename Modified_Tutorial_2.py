'''
Modified Tutorial 2: Finding the Most Frequent Word with Custom Partitioner: 
This implements the custom partitioner based on word length in finding the most frequent word. Here we can adjust the number of reducers for the job.
Input: Text files with varying sizes
Output: Most used or frequent word with its count
'''


from mrjob.job import MRJob  # Import the MRJob class from the mrjob library
from mrjob.step import MRStep  # Import the MRStep class for defining steps in the job
import re  # Import the regular expression module
import time

# Compile a regular expression pattern to match words
WORD_RE = re.compile(r"[\w']+")

class MRMostUsedWordWithCustomPartitioner(MRJob):  # Define a new class that inherits from MRJob

    def configure_args(self):
        """Define custom arguments such as the number of reducers."""
        super(MRMostUsedWordWithCustomPartitioner, self).configure_args()
        self.add_passthru_arg('--num-reducers', type=int, default=3, help="Number of reducers")

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
        # Send all (num_occurrences, word) pairs to the same reducer.
        yield None, (sum(counts), word)

    def reducer_find_max_word(self, _, word_count_pairs):  # Define the reducer function for finding the max word
        # Each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield "Most Frequent Word", max(word_count_pairs)

    def get_partition(self, word):
        """
        Custom partitioning logic based on word length.
        This will ensure words of similar lengths are grouped together.

        :param word: The word to partition.
        
        :return: An integer representing the partition ID.

        """
        word_length = len(word)
        num_reducers = self.options.num_reducers  # Retrieve number of reducers which is assigned initially
        
        # Partitioning based on word length modulo the number of reducers ie. word length modulo number of reducer
        return word_length % num_reducers



if __name__ == '__main__': 
    job = MRMostUsedWordWithCustomPartitioner(args=['project_gutenberg_eBook_emma.txt'])  # Run the MRMostUsedWordWithCustomPartitioner job
    start_time_with_combiner = time.time()
    # Run the job
    with job.make_runner() as runner:
        runner.run()
        
        # Collect and print the output inside the runner block
        for key, value in job.parse_output(runner.cat_output()):
            print(f'{key}: {value}')


    end_time_with_combiner = time.time()
    total_time_with_combiner = end_time_with_combiner - start_time_with_combiner
    print(f"Total job execution time with partitioner: {total_time_with_combiner} seconds")


