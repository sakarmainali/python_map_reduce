'''
Tutorial 2: Frequent Word Count
This example builds on the word count and it does not just count all the words, it finds the single word that appears the most throughout all the text files. 
Input: A simple text file with random words. 
Output: Most used or frequent word with its count
'''

from mrjob.job import MRJob  # Import the MRJob class from the mrjob library
from mrjob.step import MRStep  # Import the MRStep class for defining steps in the job
import re  # Import the regular expression module

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

if __name__ == '__main__': 
    MRMostUsedWord.run()  # Run the MRMostUsedWord job