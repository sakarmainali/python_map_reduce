
### Exploring the Impact of Input Data, Partitioning, and Combiners on MapReduce Performance using Python mrjob library

This project investigates how variations in input data, custom partitioning, and the use of combiners influence the efficiency of MapReduce-based data processing. The study is conducted using the Apache Hadoop implementation framework and the MapReduce algorithm, which is widely used for large-scale parallel processing due to its scalability and fault tolerance.

The project includes three stages: baseline implementations ( Tutorials : Word Count, Most Common Word, Top Employee Salaries), input data modifications, and performance optimizations through custom partitioners and combiners. Key metrics analyzed include execution time, CPU utilization, startup overhead, data shuffling overhead and sequential scanning speeds. 

Results from this work demonstrate how data size, complexity, and processing strategies affect the performance of MapReduce tasks, offering insights into optimizing distributed processing workflows.

Datasets Used:

1. [Demo input](https://github.com/zdata-inc/HadoopWithPython/blob/master/resources/input.txt) : A random text input file
    - Filename: demo_input.txt
2. [Ebook from Project Gutenberg](https://www.gutenberg.org/ebooks/) : A text ebook file
    - [Ebook titled "emma" written by Jane Austen posted on project gutenberg website](https://www.gutenberg.org/ebooks/158)
      - Filename: project_gutenberg_ebook_emma.txt
3. [Employee Salaries Dataset](https://github.com/zdata-inc/HadoopWithPython/blob/master/resources/salaries.csv): A csv file dataset containing employee data including names and salaries where we aim to compute employee top annual salaries and gross pay.
    - [Original File from github](https://github.com/zdata-inc/HadoopWithPython/blob/master/resources/salaries.csv)
      - Filename: salaries.csv

     - [First modified file (size: 1 MB)]()
        - Filename: Tutorial_3_Input_1.csv

     - [First modified file (size: 8 MB)]()
        - Filename: Tutorial_3_Input_2.csv

     - [First modified file (size: 16 MB)]()
        - Filename: Tutorial_3_Input_3.csv

     - [First modified file (size: 32 MB)]()
        - Filename: Tutorial_3_Input_4.csv      
      

4. [Wikipedia Dump](https://www.kaggle.com/datasets/toastedalmonds/wikipedia-dump-20200820) : Wikipedia text data for large input size test. The whole dataset is spilt into chunks for test runs.

    - [Test Chunk of Original Dump File](https://github.com/zdata-inc/HadoopWithPython/blob/master/resources/salaries.csv)
      - Filename: wikipedia-dump_chunk_1_mini.txt

     - [Chunk of Original Dump File (size: 1 MB)]()
        - Filename: Tutorial_1_2_Input_1.txt

     - [Chunk of Original Dump File (size: 8 MB)]()
        - Filename: Tutorial_1_2_Input_2.txt

     - [Chunk of Original Dump File (size: 16 MB)]()
        - Filename: Tutorial_1_2_Input_3.txt

     - [Chunk of Original Dump File (size: 32 MB)]()
        - Filename: Tutorial_1_2_Input_4.txt   


Necessary Software & Tools :

- [Python 3](https://www.python.org/downloads/)

- [Apache Hadoop](https://hadoop.apache.org/releases.html)
  
- [mrjob library](https://mrjob.readthedocs.io/en/latest/)

  Tested & Verified on : 
    - python-3.12.4
    - hadoop-3.3.1
    - mrjob-0.7.4


#### Setup Instruction

* Copy this project repository to your local machine
  ```shell
  git clone 
  ```
   OR
  you can download the zip file and extract in local machine





In the directory where you placed the project repo,

* Install all required python packages:
  ```shell
  pip install -r requirements.txt
  ```

* Setup Configuration File

  - Create a mrjob configuration file in project repo and name it to .mrjob.conf
    The content should look like below:

    ```shell
    runners:
      local:
        python_bin: 'C:/Users//PROJECTS/files/.venv/Scripts/python.exe'  ##[Modify the link to include your python executable file]
      hadoop:
        hadoop_streaming_jar: 'C:/hadoop-3.3.1/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar' ##[Modify the link to include the hadoop streaming jar file found in hadoop installation directory]
        python_bin: 'C:/Users//PROJECTS/files/.venv/Scripts/python.exe' ##[Modify the link to include your python executable file]
    ```

* Running the Base Tutorials:

  Base Tutorial Scripts:

    - Tutorial 1 : Word Count
    
        -File name: Tutorial_1_word_count.py 
    - Tutorial 2 : Frequent Word Count
    
        -File name: Tutorial_2_frequent_word_count.py 

    - Tutorial 3 : Top Salary
    
        -File name: Tutorial_3_top_salary.py 


  ```shell
  
  # Run by simulating Hadoop locally

  python [script_name] --runner=local --conf-path .mrjob.conf --no-bootstrap-mrjob  [input_filename]

  E.g. python Tutorial_1_word_count.py --runner=local --conf-path .mrjob.conf --no-bootstrap-mrjob demo_input.txt

  # Run on actual Hadoop cluster 
  python [script_name] --runner=hadoop --conf-path .mrjob.conf --no-bootstrap-mrjob  [input_filename]

  E.g. python Tutorial_1_word_count.py --runner=hadoop --conf-path .mrjob.conf --no-bootstrap-mrjob  demo_input.txt

  ```
* Running the Modified Tutorials:

  Modified Tutorial Scripts:

    - Modified Tutorial 1 : Basic Word Count with modified Input
    
        -File name: Modified_Tutorial_1.py 
    - Modified Tutorial 2 : Frequent Word with custom Partitioner
    
        -File name: Modified_Tutorial_2.py 

    - Modified Tutorial 3 : Top Salary with combiner
    
        -File name: Modified_Tutorial_3.py 

  ```shell
  # Run by simulating Hadoop locally

  python [script_name] --runner=local --conf-path .mrjob.conf --no-bootstrap-mrjob  [input_filename]

  E.g. python Modified_Tutorial_1.py --runner=local --conf-path .mrjob.conf --no-bootstrap-mrjob demo_input.txt

  # Run on actual Hadoop cluster 
  python [script_name] --runner=hadoop --conf-path .mrjob.conf --no-bootstrap-mrjob  [input_filename]

  E.g. python Modified_Tutorial_1.py --runner=hadoop --conf-path .mrjob.conf --no-bootstrap-mrjob  demo_input.txt

  ```
* Running the Experiments:

  ```shell
  # Run by simulating Hadoop locally

  python [script_name] --runner=local --conf-path .mrjob.conf --no-bootstrap-mrjob  [input_filename]

  E.g. python Duplicated_Experiment_1.py --runner=local --conf-path .mrjob.conf --no-bootstrap-mrjob demo_input.txt

  # Run on actual Hadoop cluster 
  python [script_name] --runner=hadoop --conf-path .mrjob.conf --no-bootstrap-mrjob  [input_filename]

  E.g. python Experiment_1.py --runner=hadoop --conf-path .mrjob.conf --no-bootstrap-mrjob  demo_input.txt



