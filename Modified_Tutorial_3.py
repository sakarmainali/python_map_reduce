'''
Modified Tutorial 3: Top Employee Salaries with Combiner Optimization
This modifies tutorial 3 and implements combiner function to partially aggregate salary data within the Map phase. 
This allows us to test whether the combiner effectively reduces intermediate data and improves performance for complex data manipulation tasks. 

Input: A dataset containing employee data including names and salaries. to compute employee top annual salaries and gross pay.
Output: It calculates and prints out top 10 annual salaries in ascending order and sums up the total payroll 
'''

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

cols = 'Name,JobTitle,AgencyID,Agency,HireDate,AnnualSalary,GrossPay'.split(',')

class TopSalariesWithCombiner(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        # Convert each line into a dictionary for easier access
        row = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))

        # Extract the salary and clean it (remove dollar sign)
        try:
            salary = float(row['AnnualSalary'][1:])
        except ValueError:
            self.increment_counter('warn', 'missing salary', 1)
            return

        # Yield the salary and the corresponding line
        yield 'salary', (salary, line)
        
        # Yield the salary for total payroll computation
        yield 'total_payroll', salary

    def combiner(self, key, values):
        if key == 'salary':
            # Partial aggregation for top 10 salaries at the combiner stage
            top_salaries = []
            for value in values:
                top_salaries.append(value)
                top_salaries.sort(reverse=True)  # Sort in descending order
                top_salaries = top_salaries[:10]  # Keep only the top 10

            # Emit partial results for further reduction
            for salary in top_salaries:
                yield key, salary
        elif key == 'total_payroll':
            # Sum partial payrolls in the combiner to reduce intermediate data
            yield key, sum(values)

    def reducer(self, key, values):
        if key == 'salary':
            # Final aggregation for top 10 salaries across all mappers
            top_salaries = []
            for value in values:
                top_salaries.append(value)
                top_salaries.sort(reverse=True)  # Sort in descending order
                top_salaries = top_salaries[:10]  # Keep only the top 10

            # Yield the top 10 salaries
            for salary in top_salaries:
                yield key, salary
        elif key == 'total_payroll':
            # Sum the total payroll across all mappers
            total_payroll = sum(values)
            yield key, total_payroll

if __name__ == '__main__':
    # Run the MRMostUsedWordWithCustomPartitioner job
    TopSalariesWithCombiner().run()

