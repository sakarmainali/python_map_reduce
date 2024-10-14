'''
Modified Tutorial 1: Basic Word Count with Modified Input
This is same as tutorial 1 but here the input data is varied  
Input: A simple text file with random words. 
Output: Total no of characters, words and lines count

'''

from mrjob.job import MRJob  # Import the MRJob class from the mrjob library
from mrjob.step import MRStep  # Import the MRStep class for defining steps in the job
import time

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
    # Create an instance of the MRWordFrequencyCount job
    job = MRWordFrequencyCount(args=['internet_archive_scifi_v3.txt'])  # Pass the input file name here

    # Run the job
    with job.make_runner() as runner:
        runner.run()

        # Collect and print the output inside the runner block
        for key, value in job.parse_output(runner.cat_output()):
            print(f'{key}: {value}')