'''
Tutorial 1: Word Count
This is the simplest example tutorial which counts how many times each word appears in a large text file along with line and character count.
Input: A simple text file with random words. 
Output: Total no of characters, words and lines count
'''

from mrjob.job import MRJob  # Import the MRJob class from the mrjob library

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

if __name__ == '__main__':
    MRWordFrequencyCount.run()  # Run the MRWordFrequencyCount job