package com.srcd.swivel

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.util.Properties


/**
  * Distributed data pre-processing for Swivel algorithm https://arxiv.org/abs/1602.02215
  *
  * Is equivalent to prep.py (and fastprep) from
  * https://github.com/tensorflow/models/tree/master/swivel#preparing-the-data-for-training
  */
object SparkPrep {

  val _minCount = 5
  val _maxVocab = 4096 * 64
  val _shardSize = 4096

  /*
   * Histogram:  word -> freq
   * Dictionary: id -> (word, freq) _ordered_ seq of word freq tupes
   * Vocabulary: word -> id _un-ordered_ Map
   */

  def buildHistogram(rdd: RDD[String]): Seq[(String, Int)] = {
     rdd.flatMap(_.split("\t"))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .collect()
        .sortBy(-_._2)
  }

  def dictFromHist(
      hist: Seq[(String, Int)],
      minCount: Int = _minCount,
      maxVocab: Int = _maxVocab,
      shardSize: Int = _shardSize
  ): Seq[(String, Int)] = {
    var numWords = Math.min(hist.length, maxVocab)
    if (numWords % shardSize != 0) {
      numWords -= numWords % shardSize
    }
    hist
      .filter(_._2 >= minCount)
      .take(numWords)
  }

  def vocabFromDict(dict: Seq[(String, Int)]): Map[String, Int] = {
    val vocab = dict.view
      .zip(Stream from 1)
      .map { case ((word, _), id) =>
        (word, id)
      }
      .toMap
    vocab
  }

  /**
    * Builds vocabulary: a map of word -> id
    *
    * @param rdd
    * @param minCount
    * @param maxVocab
    * @param shardSize
    * @return (vocabulare, dictionary)
    */
  def buildVocab(rdd: RDD[String], minCount: Int = 5, maxVocab: Int = 4096 * 64, shardSize: Int = 4096)
  : (Map[String, Int], Seq[(String, Int)]) = {
    val hist = SparkPrep.buildHistogram(rdd)
    val dict = SparkPrep.dictFromHist(hist, minCount, maxVocab, shardSize)
    val vocab = SparkPrep.vocabFromDict(dict)
    (vocab, dict)
  }



}

object SparkPrepDriver {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: " + this.getClass.getSimpleName +" <inputFile>")
      System.exit(1)
    }

    //TODO(bzz): CLI args
    //  --input <filename>
    val input = args(0)
    //  --output_dir <directory>
    //  --shard_size <int>
    //  --min_count <int>
    //  --max_vocab <int>
    //  --vocab <filename>
    //  --window_size <int>

    val sparkMaster = Properties.envOrElse("MASTER", "local[*]")
    val (sc, spark) = getContext(sparkMaster)

    //create word->id map
    val (wordToId, dict) = SparkPrep.buildVocab(sc.textFile(input))

    sc.broadcast(wordToId)

    dict.foreach { case (word, _) =>
      println(s"$word")
    }

    //Optimisations
    // 4b pointers: -XX:+UseCompressedOops if <32Gb RAM
    // tune DataStructures: http://fastutil.di.unimi.it


    //Build sharded co-ocurance matrix
    // for each token => ( ( row,                col,                   val), (row, col, val), ...)
    //                => ( ((row%i, row mod i), (col%i, col mod i),     val))
    //                => ( row%i "+" col%j,     (row mod i, col mod i, val) )
    //
    //push sort to shuffle
    // repartitionAndSortWithinPartitions(...)
    // make sure it's partition by row%i,col%j

  }


  def getContext(sparkMaster: String): (SparkContext, SparkSession) = {
    //conf.registerKryoClasses(Array(classOf[xxxx]))

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master(sparkMaster)
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._
    (spark.sparkContext, spark)
  }

}