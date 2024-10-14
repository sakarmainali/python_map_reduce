
'''
Duplicated Experiment 1: Startup Overhead: 
Here we measure how long it takes to start a MapReduce job in mrjob python. 

Input: Varied input text files
Output : Startup overhead measurement of each text files

'''


from mrjob.job import MRJob
import time

class MRWordCount(MRJob):
    def mapper(self, _, line):
        yield 'chars', len(line)
        yield 'words', len(line.split())
        yield 'lines', 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    # Measure startup overhead
    start_time = time.time()
    mr_job = MRWordCount(args=['demo_input.txt'])
    
    with mr_job.make_runner() as runner:
        runner.run()
    
    end_time = time.time()
    startup_overhead = end_time - start_time
    print(f"Startup overhead: {startup_overhead} seconds")
