The 2 Parts of the assignment are in their respective folders.

The jar file, .sbt file and the data are placed directly in the respective folders for easier access.
While the source codes are in the src/main/scala folders of each part.

The output files are also stored in the respective folders under the "Output files" sub-folder
The output files can be accessed through a text editor

For Part 1 of the assignment:

- Create a bucket in Amazon S3

- Upload the "pagerank_2.11-0.1.jar" to Amazon S3. The jar file is inside the CS6350_Pagerank folder

- Upload the "111538186_T_ONTIME_REPORTING.csv" file to S3 bucket as well. This is in the same folder as the jar file.

- Create a cluster in Amazon EMR having Spark 2.4.6

- After creation of the cluster, click on steps tab and Add steps to run:
	- Select Spark application in Step type
	- In Spark-submit options, enter --class "PageRank"
	- Enter the jar file location in AWS S3 in the Application location
	- In Arguments, first enter the location of data in the AWS S3 bucket, second enter the number of iterations and third enter the output folder in the S3 bucket 
	
- Click on Add and let the Step complete and then check the S3 bucket for outputs

- Inside the output folder are the generated part-0000x files


For Part 2 of the assignment:

- Create a bucket in Amazon S3

- Upload the "tweet_pc_pipeline_2.11-0.1.jar" to Amazon S3. The jar file is inside the CS6350_TweetPC folder.

- Upload the "Tweets.csv" file to S3 bucket as well. This is in the same folder as the jar file.

- Create a cluster in Amazon EMR having Spark 2.4.6

- After creation of the cluster, click on steps tab and Add steps to run:
	- Select Spark application in Step type
	- In Spark-submit options, enter --class "TweetPC"
	- Enter the jar file location in AWS S3 in the Application location
	- In Arguments, first enter the location of data in the AWS S3 bucket, second enter the output folder in the S3 bucket 
	
- Click on Add and let the Step complete and then check the S3 bucket for outputs

- Inside the output folder are the generated part-0000x files

