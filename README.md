# Success_at_door
The analysis of GA data
####Data Engineering test
###Approach/Logic
1. Created .py files: insights.py where all the reading input, data quality checks and loading output to directory happens.
2. Defined a schema according to JSON structure and read data using the schema so that defining data types will be better.
3. Loading the final dataset to a target location in csv file format.
4. Logging is defined on INFO level.

sql_analysis.sql contains the sql queries asked in first part of the assignment, I have also created an additional query for the purpose of giving summary stats like revenue, bounces, newvisits etc using GA data which can be visualized.

I was unable to solve the problem 4 using python code, it was not running fine but I have understood the solution and I can discuss that with a pseudocode.


###Assumptions
1.Cluster manager = YARN
2. Spark configs(Driver memory, executors memory, cores) are hypothetical as complete data size is not know.

###Run Code
Contents under runnable_app folder has all the files needed to run the program.

Copy contents of runnable folder on the edge node.

Run below command to execute our pyspark job:

sh run.sh

Data Quality:
Defined the schema and read the data according to parquet column tags to avoid any column shifting due to escape characters.

Possible Performance issue:
In case of small file issue, we can repartition data on extractor level while reading the source. In case of application taking more time, we can minimise use of UDFs as it does serialization process.

Scheduling:
Created airflow file to schedule the job daily. Please provide the source and target location in airflow DAG before keeping them under dag folder of Airflow.

#####CICD Before implementing CICD, we need to make sure few things: 
1. Packaging pyspark jobs - Implemented in runnable_app folder in jobs.zip 
2. Handling all dependencies - If we are using some external package we should make sure they are installed on edge node, hence have a requirements.txt to name package and use command - "pip install -r requirements.txt -t ./src" 
3. Unit testing for Iterative development. 
###First way: 
After above things are implemented, we can create a jenkins file which will contain three stages 
1. Build: Packages zipped folders and set the app for running(MakeFile) 
2. Run: Run the run.sh file which is having spark-submit command. 
3. Postprocess: Takes the process exit code from run stage and does the emailing(if required) and cleans the extra directories which are created while running the application

###Second way
CICD using AWS
1. Deploy the application to a QA stage after a commit is performed to the source code.
2. Perform a unit test using Spark local mode.
3. Deploy to a dynamically provisioned Amazon EMR cluster and test the Spark application on it
4. Update the application as an AWS Service Catalog product version, allowing a user to deploy any version (commit) of the application on demand.
