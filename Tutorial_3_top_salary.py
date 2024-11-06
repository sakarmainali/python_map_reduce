'''
Tutorial 3: Top Employee Salaries
This example analyzes data about employees like their net annual salaries, gross pay and calculates the highest salary and gross amount. 
This example involves more complex data manipulation compared to the first two. It might need to handle different data formats for salaries, calculate sums, and sort the results. 

Input: A dataset containing employee data including names and salaries. to compute employee top annual salaries and gross pay.
Output: It calculates and prints out top 10 gross pay as well as top 10 annual salaries in ascending order.  
'''

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

cols = 'Name,JobTitle,AgencyID,Agency,HireDate,AnnualSalary,GrossPay'.split(',')

class salarymax(MRJob):

    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, [ a.strip() for a in next(csv.reader([line]))]))

        # Yield the salary
        yield 'salary', (float(row['AnnualSalary'][1:]), line)
        
        # Yield the gross pay
        try:
            yield 'gross', (float(row['GrossPay'][1:]), line)
        except ValueError:
            self.increment_counter('warn', 'missing gross', 1)

    def reducer(self, key, values):
        topten = []

        # For 'salary' and 'gross' compute the top 10
        for p in values:
            topten.append(p)
            topten.sort()
            topten = topten[-10:]

        for p in topten:
            yield key, p

    combiner = reducer

if __name__ == '__main__':
    salarymax.run()