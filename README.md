Partitioning data
___________________

Data can be repartitioned based on specific columns of interest as follow 

 sparkDataFrame.repartition(*cols) 

Spark will detect unique values across the specified columns and partition them accordingly. So depending on the number of unique values for the specified columns those many partitions will be created(If you were expecting a lower number of partitions and noticed 200 partitions being created, its a spark default setting which can be controlled using spark.sql.shuffle.partitions property. Only the expected number of partitions will have data and you will see the rest of the tasks quickly running to completion). 

In spark, partitions equate to tasks and these are the small units that will be distributed across worker nodes to perform the computation. Repartitioning will involve data shuffling between worker nodes to ensure the columns with the same values are all moved to the same executor. For eg:- if you are partitioning on a department number with unique values 10,11,12, the spark will move data in such a way that each partition will have only one department number. One common mistake that can slow down this step is loading unnecessary columns to spark Dataframe and not getting rid of them before performing partitioning. The above use cases had 300+ columns while only 150 were required in the actual computation. By ensuring only required columns that are needed to perform forecasting the data size will reduce significantly and speed up the partitioning process. 

 

Shipping preferred libraries to executors 
___________________________________________
Create CONDA env and create a dist of that env. Use that dist in spark. 

 

Adding logic for forecasting per partition
__________________________________________

Once the repartition happened the logic to perform forecasting can be added to the mapPartitions method. The logic for model fitting and prediction can remain the same as it was written even before using Spark. Spark will take care of executing the same logic on multiple partitions on multiple nodes in parallel.  

sparkDataFrame.rdd.mapPartitions(forecasting_logic) 
def forecasting_logic(partition_list): 
    pdf = pd.DataFrame(partition_list) 
    model=your_prefferred_lib.fit(pdf) 
    predicted_pdf = model.predict() 
    return predicted_pdf.values.tolist() 

 

Deciding on spark executors/cores and memory 
____________________________________________
One of the common question that arises while porting code to pyspark is how to decide on how much resources is required. While this totally depends on the use case but some common guidelines can be followed. 

Performing simple spark SQL to do a count after performing group by on the specific columns on which partitioning to be done will give a hint on the number of records a single task will be handling. Looking at this figure along with an understanding of data will help to arrive at a ballpark figure on the amount of memory a single task will need. One thing to keep in mind is this memory cannot exceed the total memory available for an executor node in the Hadoop cluster. For example, if maximum memory allotted for a single node in your Hadoop cluster is 16 GB, then we cannot execute the task which needs a bigger memory requirement. 

 

Let's look at a use case example 

Assume we have a data which we are partitioning on department id. If there 20 unique values for the department we will end up with 20 partitions. Now if we can allot 20 cores all the 20 partitions can be computed in parallel. We can provide a configuration of 4 executors and 5 cores per executor(4*5=20). A maximum of 5 cores per executor is a recommend configuration to start with. However, this means 5 tasks will be running at a time within one executor. If data for all 5 tasks cannot fit in memory, ensure to give lesser cores and give more executors. For eg:- 10 executors and 2 cores per executor(10*2=20). 

Assigning as many executors and cores as the number partitions may not be feasible in all scenarios. In such cases see what is the maximum executors and cores that can be allotted to this job from the dedicated queue without affecting any other job for the same queue 

 

Broadcast small data/variables 
______________________________

Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset in an efficient manner. Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication costs.  

item_attribute_data=item_attribute_data.collect() 
sc.broadcast(item_attribute_data) 
# Above will broadcast item_attribute_data to all nodes of cluster. 

 

Monitoring pyspark jobs 
________________________

Spark UI provides sufficient details on how the job is progressing. To ensure your jobs are getting the specified number of resources(cores/memory), take a look at the executor tab. You might have to adjust the memory requirements, executors or cores depending on your job. Looking at the logs in spark UI you will be able to figure out if a lot of tasks are failing due to out of memory. In such case provide more memory or reduce cores per executor 
