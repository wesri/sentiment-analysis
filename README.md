# sentiment-analysis
Sentiment Analysis of Tweets Using Microsoft Azure

Setting Up the Pipeline on Cloudera
-----------------------------------

First follow [Cloudera's tutorial](https://github.com/cloudera/cdh-twitter-example) with a few exceptions:

1. Build `hive-serdes` and `flume-sources` yourself.

2. Create the tweets table using following commands:

  <pre>
  ADD JAR /usr/lib/hadoop/hive-serdes-1.0-SNAPSHOT.jar;

  CREATE EXTERNAL TABLE tweets (
    id BIGINT,
    created_at STRING,
    lang STRING,
    source STRING,
    favorited BOOLEAN,
    retweeted_status STRUCT<
      text:STRING,
      user:STRUCT<screen_name:STRING,name:STRING>,
      retweet_count:INT>,
    entities STRUCT<
      urls:ARRAY<STRUCT<expanded_url:STRING>>,
      user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
      hashtags:ARRAY<STRUCT<text:STRING>>>,
    text STRING,
    user STRUCT<
      screen_name:STRING,
      name:STRING,
      friends_count:INT,
      followers_count:INT,
      statuses_count:INT,
      verified:BOOLEAN,
      utc_offset:INT,
      time_zone:STRING>,
    in_reply_to_screen_name STRING
  )
  PARTITIONED BY (datehour INT)
  ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
  LOCATION '/user/flume/tweets';
  </pre>

3. Modify this project's `TweetUpload.java` file by replacing following strings:
  ```
  account_name_here --> Your Azure Blob storage account name
  account_key_here --> Your key for Azure Blob storage
  container_name_here --> Container name
  rdp_username_here --> Your RDP username
  ```
  After modifying build the file.

4. Replace the `oozie-workflows` folder with this project's `oozie-workflows` folder and move your built `TweetUpload.jar` to `oozie-workflows/lib/`.

Setting up Spark on Azure HDInsight
-----------------------------------
1. Download wordlists made by Minqing Hu and Bing Liu from [here](http://www.cs.uic.edu/~liub/FBS/sentiment-analysis.html#lexicon). Create a directory `SentimentAnalysis` and place the word lists there.

2. Set variable `numberOfPartitions` on `Analysis.scala` and build a JAR file. Put the JAR file into the same directory as the word lists.

3. Provision an HDInsight Spark cluster by following [this](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-apache-spark-zeppelin-notebook-jupyter-spark-sql/#provision) tutorial's section "Provision an HDInsight Spark cluster".

4. Use RDP to connect to the cluster and place the directory `SentimentAnalysis` under `C:\`.

Run the application on Azure HDInsight
------------------------------

When running the application, it reads all the tweet files in folder `analysisFiles` and  combines the files to `tweetFile.tsv`.

To run it, open a Hadoop command line and execute following commands:
<pre>
cd C:\SentimentAnalysis
C:\apps\dist\spark-1.3.1.2.2.7.1-0004\bin\spark-submit --class "Analysis" --master spark://headnodehost:7077 --executor-memory 4G --total-executor-cores 40 JAR_NAME_HERE.jar
</pre>

NOTE: You may want to change the number of cores and memory. Also change the Spark version number to correspond to version that you are using.

Output file is a .tsv file and it contains the numbers of positive and negative tweets for each hour and the most used positive and negative hashtags.

The format is: `time negative_tweets positive_tweets negative_hashtags positive_hashtags`

Example: `2015-08-15 23  2034  2510  #bad  #happy #wow`


