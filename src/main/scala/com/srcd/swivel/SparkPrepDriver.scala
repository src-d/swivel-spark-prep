package com.srcd.swivel

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.util.Properties


object SparkPrep {

  val _minCount = 5
  val _maxVocab = 4096 * 64
  val _shardSize = 4096

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

  def buildDict(rdd: RDD[String], minCount: Int = 5, maxVocab: Int = 4096 * 64, shardSize: Int = 4096): Seq[(String, Int)] = {
    val hist = SparkPrep.buildHistogram(rdd)
    val dict = SparkPrep.dictFromHist(hist, minCount, maxVocab, shardSize)
    dict
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

    //create id->word, and word->id dicts
    val dict = SparkPrep.buildDict(sc.textFile(input))

    dict.foreach { case (word, freq) =>
      println(s"$word $freq")
    }

    //Optimisations
    // 4b pointers: -XX:+UseCompressedOops if <32Gb RAM
    // tune DataStructures: http://fastutil.di.unimi.it

    // broadcast id->word, and word->id dicts

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