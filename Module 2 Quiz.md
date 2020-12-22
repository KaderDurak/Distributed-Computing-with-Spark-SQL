Question 1
What are the different units of parallelism? (Select all that apply.)

**Task**

**Executor**

**Core**

**Partition**


Question 2
What is a partition?


**A portion of a large distributed set of data**

A synonym with "task"

A division of computation that executes a query

The result of data filtered by a WHERE clause


Question 3
What is the difference between in-memory computing and other technologies? (Select all that apply.)


**In-memory operations were not realistic in older technologies when memory was more expensive**

In-memory computing is slower than other types of computing

**In-memory operates from RAM while other technologies operate from disk**

**Computation not done in-memory (such as Hadoop) reads and writes from disk in between each step**


Question 4
Why is caching important?


 It always stores data in-memory to improve performance


**It stores data on the cluster to improve query performance**


 It reformats data already stored in RAM for faster access


 It improves queries against data read one or more times


Question 5
Which of the following is a wide transformation? (Select all that apply.)


**ORDER BY**

**GROUP BY**

SELECT

WHERE


Question 6
Broadcast joins...


Shuffle both of the tables, minimizing data transfer by transferring data in parallel

Shuffle both of the tables, minimizing computational resources

**Transfer the smaller of two tables to the larger, minimizing data transfer**

Transfer the smaller of two tables to the larger, increasing data transfer requirements



Question 7
When is it appropriate to use a shuffle join?



**When both tables are moderately sized or large**

 When both tables are very small

  Never. Broadcast joins always out-perform shuffle joins.

  When the smaller table is significantly smaller than the larger table


Question 8
Which of the following are bottlenecks you can detect with the Spark UI? (Select all that apply.)


**Shuffle reads**

**Data Skew**

Incompatible data formats

**Shuffle writes**


Question 9
What is a stage boundary?


An action caused by a SQL query is predicate

Any transition between Spark tasks

A narrow transformation

**When all of the slots or available units of processing have to sync with one another**


Question 10
What happens when Spark code is executed in local mode?


A cluster of virtual machines is used rather than physical machines

**The executor and driver are on the same machine**

The code is executed in the cloud

The code is executed against a local cluster
