/*  Copyright 2015 Aalto University, Hussnain Ahmed

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

import scala.io.Source
import scala.collection.mutable.Buffer
import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import java.io.File
import scala.collection.mutable.Buffer

object Analysis {

  val positiveFile = "wordFiles/positive.txt"
  val negativeFile = "wordFiles/negative.txt"
  val outputFolder = "output"

  val tweetFolder = "analysisFiles"

  def main(args: Array[String]) {

    def merge(source: String, destination: String): Unit = { // merge two files
      val newConfig = new Configuration()
      val hdfs = FileSystem.get(newConfig)
      FileUtil.copyMerge(hdfs, new Path(source), hdfs, new Path(destination), true, newConfig, null)
    }

    def readWordFile(fileName: String): Vector[String] = { // For reading word lists
      val wordFile = Source.fromFile(fileName)
      val tmpWords = Buffer[String]()
      try {
        for (line <- wordFile.getLines()) {
          val trimmedLine = line.takeWhile(a => a != ';').trim // ';' means comment
          if (trimmedLine != "") tmpWords += trimmedLine
        }
      } finally {
        wordFile.close()
      }
      tmpWords.toVector
    }

    val posWords = readWordFile(positiveFile)
    val negWords = readWordFile(negativeFile)

    def getSentiment(tweetText: String): Int = { // Calculate tweet sentiment

      val tweetWords = tweetText.toLowerCase().split(' ')
      var tweetSentiment = 0
      val neutralWords = Buffer[String]()

      for (word <- tweetWords) { // Compare each word with negative and positive word lists
        if (posWords.contains(word)) {
          tweetSentiment += 1
        } else if (negWords.contains(word)) {
          tweetSentiment -= 1
        }
      }
      tweetSentiment
    }

    val newConfig = new Configuration()
    val hdfs = FileSystem.get(newConfig)

    val file = outputFolder + "/tmpResults"
    val finalFile = outputFolder + "/results.tsv"

    hdfs.delete(new Path(file), true) // Delete earlier files if those exist
    hdfs.delete(new Path(finalFile), true)

    val tmpTweetFolder = "tmpAnalysisFiles"
    val newTweetFileName = "tweetFile.tsv"
    val tmpTweetFile = tmpTweetFolder + "/" + newTweetFileName
    val newTweetFile = tweetFolder + "/" + newTweetFileName

    merge(tweetFolder, tmpTweetFile) // Merge input files to one file

    hdfs.rename(new Path(tmpTweetFolder), new Path(tweetFolder))

    val conf = new SparkConf().setAppName("Tweet Analysis")

    val sc = new SparkContext(conf)

    val tweetFile = sc.textFile(newTweetFile, 8).cache()

    val allowedTabs = 2

    val tweetData: RDD[(String, String, String, Int)] = tweetFile.mapPartitions(lines => {

      lines.map(tweetLine => {
        val line = tweetLine.drop(tweetLine.takeWhile(c => !c.isDigit).length)
        val tabs = line.count(_ == '\t')
        val parser = new CSVParser('\t')
        val tweetDetails = parser.parseLine(line.replace("\"", "")) //Parse lines and remove quotation marks

        if (tabs != allowedTabs) { // In case a tweet contains a tab
          for (i <- 3 to tabs) {
            tweetDetails(2) += tweetDetails(i)
          }
        }

        if (tweetDetails.isDefinedAt(0) && tweetDetails(0) != "NULL" // Check if everything is defined
          && tweetDetails.isDefinedAt(1) && tweetDetails(1) != "NULL" && tweetDetails.isDefinedAt(2)) {

          (tweetDetails(0), tweetDetails(1), tweetDetails(2).toLowerCase, getSentiment(tweetDetails(2)))

        } else {
          ("", "", "", 0) // If something was undefined
        }

      })
    }).filter(part => part._1 != "") // Remove invalid tweets

    val tweetData2 = tweetData.map {

      case (id, date, text, sentiment) => {

        val hashtagBuffer = Buffer[String]() // For storing hashtags
        var hashtagText = text

        while (hashtagText.length > 0) {

          val drop_text = hashtagText.takeWhile(_ != '#') // Drop chars before the hashtag
          hashtagText = hashtagText.drop(drop_text.length)
          var hashtag = hashtagText.takeWhile(c => c != ' ' && c != '.' && c != ',' && c != '!' && c != '?' && c != ';' && c != ':')
          hashtagBuffer += hashtag // Store hashtag
          hashtagText = hashtagText.drop(hashtag.length) // Drop just stored hashtag from tweet text 

        }

        var simpleSentiment = 0
        if (sentiment > 0) { // Use only 1 and -1 values for sentiment
          simpleSentiment = 1
        } else if (sentiment < 0) {
          simpleSentiment = -1
        }

        val tweetTimeFormat = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy")
        val parsedTime = tweetTimeFormat.parse(date)
        val hourFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH").format(parsedTime) // Format time to new format

        (hourFormat, simpleSentiment, hashtagBuffer.toVector)

      }
    }.filter(details => details._2 != 0) // Remove neutral tweets (sentiment = 0)

    val sentimentCounts = tweetData2.map { case (date, sentiment, hashtags) => ((date, sentiment), 1) }
      .reduceByKey { case (x, y) => x + y } // Calculate tweet amount with same sentiment 
      .sortBy(_._1)
      .map { case ((date, sentiment), count) => (date, (sentiment, count)) }
      .groupByKey() // Group by date
      .map { case (date, sentimentCount) => (date, sentimentCount.map { case (sentiment, count) => count }.mkString("\t")) }
    // Separate amounts of negative and positive tweets by tab

    val tweetData3 = tweetData2.map {
      case (hourFormat, sentiment, hashtags) => {
        hashtags.map(tag => (hourFormat, sentiment, tag))
      }
    }
      .flatMap(identity) // Flatten (not needed to be a vector)

      .filter(_._3 != "") // Remove empty hashtags
      .map(y => (y, 1))
      .reduceByKey { case (x, y) => x + y } // Count hashtag usages
      .sortBy(-_._2).map { // Sort by amount of hashtag usage
        case (dsh, count) => (dsh._1, (count, dsh._2, dsh._3)) // dsh = (date, sentiment, hashtags)
      }
      .groupByKey() // Group by date

    val dateAndHashtags = tweetData3
      .map {
        case (date, x) => (date, x.take(10).groupBy(_._2).map { // Take 10 most used hashtags and group by sentiment
          case (sentiment, details) => (sentiment, details.map(d => d._3).mkString(" ")) // Separate hashtags by space
        })
      }
      .sortBy(x => x._2(x._2.keys.toArray.head)) // Negative hashtags first
      .map {
        case (date, sentimentToHashtags) =>
          var extraPosTab = 0
          var extraNegTab = 0
          if (!sentimentToHashtags.isDefinedAt(1)) extraPosTab += 1 // In case there aren't any popular negative or positive hashtags
          if (!sentimentToHashtags.isDefinedAt(-1)) extraNegTab += 1 // we have to add tabs
          (date, "\t" * extraNegTab + sentimentToHashtags.values.mkString("\t") + "\t" * extraPosTab)
      } // Discard sentiment and separate fields by tab

    val dateAndHashtagsSorted = dateAndHashtags.sortBy(_._1)

    val tweetCounts = sentimentCounts.join(dateAndHashtagsSorted)
      .map { case (date, (counts, hashtags)) => Array(date, Array(counts, hashtags).mkString("\t")).mkString("\t") }.sortBy(identity)

    tweetCounts.saveAsTextFile(file)

    merge(file, finalFile) // Merge smaller files to one bigger file

    sc.stop()

  }

}
