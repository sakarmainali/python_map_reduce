'''
Modified Tutorial 1: Basic Word Count with Modified Input
This is same as tutorial 1 but here the input data is varied and also includes validation for diverse datasets. 
Input: A wikipedia dump text file with random words. 
Output: Total no of characters, words and lines count

'''

from mrjob.job import MRJob  # Import the MRJob class from the mrjob library
from mrjob.step import MRStep  # Import the MRStep class for defining steps in the job
import unicodedata  # For handling and normalizing Unicode text
import time

class MRWordFrequencyCount(MRJob):  # Define a new class that inherits from MRJob

    def mapper(self, _, line):  # Define the mapper function
        # Normalize the line to ensure consistent encoding (e.g., NFC form for Unicode)
        normalized_line = unicodedata.normalize('NFC', line)

        # Yield the total number of characters in the line, validating diverse encoding
        yield "Total chars count: ", len(normalized_line)

        # Split words by considering Unicode-aware whitespace
        yield "Total words count: ", len(normalized_line.split())

        # Yield the count of lines (always 1 for each line)
        yield "Total lines count: ", 1

    def reducer(self, key, values):  # Define the reducer function
        # Sum up all the values for each key and yield the result
        yield key, sum(values)

if __name__ == '__main__':
    # Create an instance of the MRWordFrequencyCount job
    MRWordFrequencyCount().run()