'''
Duplicated Experiment 2: Data Shuffling Overhead :
Here we will measure time and resources spent shuffling data between maps and reduce tasks, particularly with the new combiner function.  

Input: Varied input text files with combiner and without combiner
Output : Data Shuffling Overhead of map reduce job

'''
import time
from mrjob.job import MRJob
import psutil  # To measure memory usage (optional)

class MRWordCountWithCombiner(MRJob):
    def mapper(self, _, line):
        yield 'words', len(line.split())

    def combiner(self, key, values):
        # Combine values before sending to reducer
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

class MRWordCountWithoutCombiner(MRJob):
    def mapper(self, _, line):
        yield 'words', len(line.split())

    def reducer(self, key, values):
        yield key, sum(values)

def measure_memory():
    # Function to measure memory usage
    process = psutil.Process()
    return process.memory_info().rss / (1024 * 1024)  # Return memory usage in MB

if __name__ == '__main__':
    input_file = 'wikipedia-dump_chunk_1.txt'  # Specify your input text file here

    # Measure performance with combiner
    print("Running job with combiner...")
    start_time_with_combiner = time.time()
    start_memory_with_combiner = measure_memory()

    # Measure network I/O before shuffle starts
    net_io_before = psutil.net_io_counters()

    #Run the mrjob
    mr_job_with_combiner = MRWordCountWithCombiner(args=[input_file])
    #mr_job_with_combiner = MRWordCountWithCombiner(args=[input_file])

    with mr_job_with_combiner.make_runner() as runner:
        runner.run()

    

    end_memory_with_combiner = measure_memory()
    end_time_with_combiner = time.time()
    # Measure network I/O after shuffle ends
    net_io_after = psutil.net_io_counters()

    total_time_with_combiner = end_time_with_combiner - start_time_with_combiner
    total_memory_with_combiner = end_memory_with_combiner - start_memory_with_combiner
    data_shuffled = (net_io_after.bytes_sent - net_io_before.bytes_sent) + (net_io_after.bytes_recv - net_io_before.bytes_recv)
    data_shuffled_mb = data_shuffled / (1024 * 1024)
    

    print(f"Total job execution time with combiner: {total_time_with_combiner} seconds")
    print(f"Memory usage with combiner: {total_memory_with_combiner} MB\n")
    print(f"Data Shuffling Overhead: {data_shuffled_mb} MB")

    # Measure performance without combiner
    print("Running job without combiner...")
    start_time_without_combiner = time.time()
    start_memory_without_combiner = measure_memory()

    mr_job_without_combiner = MRWordCountWithoutCombiner(args=[input_file])
    # Measure network I/O before shuffle starts
    net_io_before = psutil.net_io_counters()
    with mr_job_without_combiner.make_runner() as runner:
        runner.run()

    end_memory_without_combiner = measure_memory()
    end_time_without_combiner = time.time()
    net_io_after = psutil.net_io_counters()# Measure network I/O after shuffle ends
    total_time_without_combiner = end_time_without_combiner - start_time_without_combiner
    total_memory_without_combiner = end_memory_without_combiner - start_memory_without_combiner
    data_shuffled = (net_io_after.bytes_sent - net_io_before.bytes_sent) + (net_io_after.bytes_recv - net_io_before.bytes_recv)
    data_shuffled_mb = data_shuffled / (1024 * 1024)


    print(f"Total job execution time without combiner: {total_time_without_combiner} seconds")
    print(f"Memory usage without combiner: {total_memory_without_combiner} MB\n")
    print(f"Data Shuffling Overhead: {data_shuffled_mb} MB")
    # Summarize the results
    print("===== Performance Comparison =====")
    print(f"Time with combiner: {total_time_with_combiner} seconds")
    print(f"Time without combiner: {total_time_without_combiner} seconds")
    print(f"Memory with combiner: {total_memory_with_combiner} MB")
    print(f"Memory without combiner: {total_memory_without_combiner} MB")
