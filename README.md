
### Exploring the Impact of Input Data, Partitioning, and Combiners on MapReduce Performance using Python mrjob library

This project investigates how variations in input data, custom partitioning, and the use of combiners influence the efficiency of MapReduce-based data processing. The study is conducted using the Apache Hadoop implementation framework and the MapReduce algorithm, which is widely used for large-scale parallel processing due to its scalability and fault tolerance.

The project includes three stages of experimentation: baseline implementations ( Tutorials : Word Count, Most Common Word, Top Employee Salaries), input data modifications, and performance optimizations through custom partitioners and combiners. Key metrics analyzed include execution time, CPU utilization, startup overhead, data shuffling overhead and sequential scanning speeds. 

Results from this work demonstrate how data size, complexity, and processing strategies affect the performance of MapReduce tasks, offering insights into optimizing distributed processing workflows.

Datasets Used:

1. [Demo input](https://github.com/zdata-inc/HadoopWithPython/blob/master/resources/input.txt) : A random text input file
    - Filename: demo_input.txt
2. [Ebook from Project Gutenberg](https://www.gutenberg.org/ebooks/158) : Ebook titled "emma" written by Jane Austen posted on project gutenberg website
    - Filename: project_gutenberg_ebook_emma.txt
3. [Employee Salaries Dataset](https://github.com/zdata-inc/HadoopWithPython/blob/master/resources/salaries.csv): A dataset containing employee data including names and salaries. to compute employee top annual salaries and gross pay. The dataset used is the example salary information from the city of Baltimore for 2014.

4. [Wikipedia Dump](https://www.kaggle.com/datasets/toastedalmonds/wikipedia-dump-20200820) : Wikipedia text data for large input size test


Necessary Software & Tools :

- [Python 3](https://www.python.org/downloads/)
- [Apache Hadoop](https://hadoop.apache.org/releases.html)
- [mrjob library](https://mrjob.readthedocs.io/en/latest/)


#### Setup Instruction

* Copy this project repository to your local machine
  ```shell
  git clone 
  ```
   OR
  you can download the zip file and extract in local machine

* Install all required packages:
  ```shell
  pip install -r requirements.txt
  ```
  
* Running the Base Tutorials:
  ```shell
  python [script_name] [input_file_name]

  E.g. python Tutorial_1_word_count.py demo_input.txt
  ```
* Running the Modified Tutorials:

  Just run the script 
  ```shell
  python [script_name]

  E.g. python Modified_Tutorial_1.py
  ```
* Running the Experiments:

  Just run the script 
  ```shell
  python [script_name]

  E.g. python  New_Experiment_1.py
  ```

* Further Instructions on running experiments

  Run the script with input file as argument.
E.g.:


