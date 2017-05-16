package com.srcd.swivel

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Partitioner

import scala.collection.mutable
import scala.util.Properties


/**
  * Distributed data pre-processing for Swivel algorithm https://arxiv.org/abs/1602.02215
  *
  * Is equivalent to prep.py (and fastprep) from
  * https://github.com/tensorflow/models/tree/master/swivel#preparing-the-data-for-training
  */
class SparkPrep {
  type Coocuarances = mutable.HashMap[(Int, Int), Float]
}

object SparkPrep {

  val defulatMinCount = 5
  val defaultMaxVocab = 4096 * 64
  val defaultShardSize = 4096
  val defaultWindowSize = 10

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
                    minCount: Int = defulatMinCount,
                    maxVocab: Int = defaultMaxVocab,
                    shardSize: Int = defaultShardSize
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

  /**
    * Converts array of tokens to Ids
    *
    * @param rdd
    * @param wordToIdVar
    * @return
    */
  def wordsToIds(rdd: RDD[Array[String]], wordToIdVar: Broadcast[Map[String, Int]]) = {
    rdd.map(line => {
        line flatMap { // flat is important, skips OOV
          wordToIdVar.value.get(_)
        }
      })
  }

  /**
    * Transforms to Id each token, and list elements of co-ocurence matrix.
    * Each matrix element (i,j) can be listed multiple times.
    *
    * @param rdd
    * @param windowSize
    * @param vocab
    * @return
    */
  def buildCoocuranceMatrix(rdd: RDD[Array[String]], windowSize: Int, vocab: Broadcast[Map[String, Int]]): RDD[((Int, Int), Double)] = {
    val ids = wordsToIds(rdd, vocab)
    val coocs = ids.flatMap(wIds => {
      generateCoocurance(wIds, windowSize)
    })
    coocs
  }

  def generateCoocurance(ids: Array[Int], windowSize: Int) = {
    val coocs = mutable.HashMap[(Int, Int), Double]().withDefaultValue(0)
      0 until ids.size foreach { pos =>
        val lid = ids(pos)
        val windowExtent = Math.min(windowSize + 1, ids.size - pos)
        1 until windowExtent foreach { off =>
          val rid = ids(pos + off)
          val count = 1.0 / off
          val pair = (Math.min(lid, rid), Math.max(lid, rid))
          coocs(pair) += count
          //sums[lid] += count
          //sums[rid] += count
        }
        //sums[lid] += 1.0
        val pair = (lid, lid)
        coocs(pair) += 0.5  // Only add 1/2 since we output (a, b) and (b, a)
      }
      //TODO(bzz): output summs as well as coocs
      coocs.map { case ((lid, rid), count) =>
        ((lid, rid), count)
      }
  }

  /**
    * For given co-ocurance matrix, shards it to <bold>numShards^2</bold> pices
    *
    * @param coocs
    * @param numShards number of shards along the dimention
    * @returnw
    */
  def doShardMatrix(coocs: RDD[((Int, Int), Double)], numShards: Int): RDD[((Int, Int), Double)] = {
    val shardedCoocs = coocs
      .repartitionAndSortWithinPartitions(new ShardPartitioner(numShards))
      .reduceByKey(_+_)

      //.map { case ((i, j), weight) =>
      //  ( (i%numShards, j%numShards), (i/numShards, j/numShards, weight) )
      //}
    shardedCoocs
  }

}

/**
  * Number of partitions is ^2 number of shards
  * @param numShards number of shards (per dimention)
  */
class ShardPartitioner(numShards: Int) extends org.apache.spark.Partitioner {
  require(numShards >= 0, s"Number of partitions ($numShards) cannot be negative.")

  def numPartitions: Int = numShards * numShards

  def getPartition(key: Any): Int = key match {
    case null => 0
    case (i, j) => (i.asInstanceOf[Int]%numShards) * numShards + j.asInstanceOf[Int]%numShards
  }

  override def equals(other: Any): Boolean = other match {
    case o: ShardPartitioner => o.numPartitions == numPartitions
    case _ => false
  }

  override def hashCode: Int = numPartitions
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
    val shardSize = SparkPrep.defaultWindowSize
    //  --min_count <int>
    //  --max_vocab <int>
    //  --vocab <filename>
    //  --window_size <int>
    val windowSize = SparkPrep.defaultWindowSize

    val sparkMaster = Properties.envOrElse("MASTER", "local[*]")
    val (sc, spark) = getContext(sparkMaster)

    //create word->id map
    val (wordToId, dict) = SparkPrep.buildVocab(sc.textFile(input))

    val wordToIdVar = sc.broadcast(wordToId)

    // write the vocab to {row, col}_vocab.txt
    dict.foreach { case (word, _) =>
      println(s"$word")
    }
    val numShards = dict.size / shardSize

    //Optimisations
    // 4b pointers: -XX:+UseCompressedOops if <32Gb RAM
    // tune DataStructures: http://fastutil.di.unimi.it
    // convert to IDs and persist (measure size/throughtput)

    //Builds co-ocurance matrix
    val coocs = SparkPrep.buildCoocuranceMatrix( // RDD[ ((i, j), weight) ]
      sc.textFile(input).map(_.split("\t")),
      windowSize,
      wordToIdVar
    )

    val shardedCoocs = SparkPrep.doShardMatrix(coocs, numShards)

    //TODO(bzz): output `{row, col}_sums.txt`

    //TODO(bzz): Outup *.pb per partion using https://github.com/tensorflow/ecosystem/tree/master/spark/spark-tensorflow-connector
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