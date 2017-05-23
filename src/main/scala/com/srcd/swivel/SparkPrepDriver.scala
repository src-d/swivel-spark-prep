package com.srcd.swivel

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

import org.apache.hadoop.io.{NullWritable, BytesWritable}

import org.tensorflow.example.{FloatList, Int64List, Feature, Features, Example}
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat

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

  val defaultMinCount = 5
  val defaultMaxVocab = 4096 * 64
  val defaultShardSize = 4096
  val defaultWindowSize = 10
  val defaultOutputDir = "/tmp/swivel-spark"

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
    hist: Seq[(String, Int)], minCount: Int = defaultMinCount, maxVocab: Int = defaultMaxVocab, shardSize: Int = defaultShardSize
  ): Seq[(String, Int)] = {

    val freqFiltered = hist.filter(_._2 >= minCount)
    var numWords = Math.min(freqFiltered.length, maxVocab)
    if (numWords % shardSize != 0) {
      numWords -= numWords % shardSize
    }
    freqFiltered.take(numWords)
  }

  def vocabFromDict(dict: Seq[(String, Int)]): Map[String, Int] = {
    val vocab = dict.view
      .zipWithIndex
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
    * @return (vocabulary, dictionary): both are word -> id mappings, dictionary is orderd by freq
    */
  def buildVocab(
    rdd: RDD[String], minCount: Int = defaultMinCount, maxVocab: Int = defaultMaxVocab, shardSize: Int = defaultShardSize
  ): (Map[String, Int], Seq[(String, Int)]) = {

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
  def buildCoocuranceMatrix(
    rdd: RDD[Array[String]],
    windowSize: Int,
    numShards: Int,
    vocab: Broadcast[Map[String, Int]]
  ): RDD[((Int, Int), Double)] = {

    val ids = wordsToIds(rdd, vocab)
    val coocs = ids.flatMap(generateCoocurance(_, windowSize))
    val sharded = mergeCoocsAndShard(coocs, numShards)

    sharded
    //sharded.map { case ((i, j), w) =>
    //  ( (i/numShards, j/numShards), w)
    //}

    //.map { case ((i, j), w) =>
    //  ( (i%numShards, j%numShards), (i/numShards, j/numShards, w) )
    //}
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
          coocs(pair.swap) += count
          //sums[lid] += count
          //sums[rid] += count
        }
        //sums[lid] += 1.0
        val pair = (lid, lid)
        coocs(pair) += 1.0  // Add 1 (not 1/2 as before) since we do output (a, b) and (b, a) separatly
      }
      //TODO(bzz): output summs as well as coocs
      coocs
  }

  /**
    * For given co-ocurance matrix, shards it to <bold>numShards * numShards</bold> pices
    *
    * @param coocs
    * @param numShards number of shards along the dimention
    * @return
    */
  def mergeCoocsAndShard(coocs: RDD[((Int, Int), Double)], numShards: Int): RDD[((Int, Int), Double)] = {
    //TODO(bzz): if time permits, compare performance of 3 possible impls below
    val shardedCoocs = coocs
      //I. repartitionAndSort + smart manual reduce over .mapPartition(), takeing advantage of key order
      //  .repartitionAndSortWithinPartitions(new ShardPartitioner(numShards))
      //  .mapPartitions { iter =>
      //    for (x <- iter) {
      //  }
      //}

      //II.
      .reduceByKey(new ShardPartitioner(numShards), _+_)
      .mapPartitions(_.toSeq.sortBy(_._1).toIterator, true)
      // or
      //.repartitionAndSortWithinPartitions(new ShardPartitioner(numShards))

      //III. partition + dumb manual 'reduce' over .mapPartitons()
      //.partitionBy(new ShardPartitioner(numShards))
      //.mapPartitions { _.toArray.groupBy(_._1).mapValues(_.map(_._2).sum).toIterator }
      //  or same, through (more performant?)
      //.mapPartitions(reducePartition) from PairRDDFunctions.reduceByKeyLocally()
    shardedCoocs
  }

}

/**
  * Number of partitions is square the number of shards
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



class Cli(arguments: Seq[String]) extends ScallopConf(arguments) {
  version("swivel-spark-prep 0.1.0 by Source{d}")
  banner("""Usage: swivel-spark-prep [OPTION]...
           |Swivel-spark-prep parallelize data pre-processing for Swivel ML model
           |Options:
           |""".stripMargin)
  footer("\nFor the details , consult https://github.com/tensorflow/models/tree/master/swivel#preparing-the-data-for-training")

  val input = opt[String](required = true)
  val output_dir = opt[String](default = Some(SparkPrep.defaultOutputDir))
  val shard_size = opt[Int](default = Some(SparkPrep.defaultShardSize))
  val min_count = opt[Int](default = Some(SparkPrep.defaultMinCount))
  val max_vocab = opt[Int](default = Some(SparkPrep.defaultMaxVocab))
  val window_size = opt[Int](default = Some(SparkPrep.defaultWindowSize))
  verify()
}


object SparkPrepDriver {

  def main(args: Array[String]): Unit = {
    val cli = new Cli(args)
    if (args.length < 1) {
      cli.printHelp()
      System.exit(1)
    }

    val sparkMaster = Properties.envOrElse("MASTER", "local[*]")
    val (sc, spark) = getContext(sparkMaster)

    val input = sc.textFile(cli.input())

    //create word->id map
    val (wordToId, dict) = SparkPrep.buildVocab(input, cli.min_count(), cli.max_vocab(), cli.shard_size())

    val wordToIdVar = sc.broadcast(wordToId)
    val numShards = dict.size / cli.shard_size()
    val shardSize = cli.shard_size()

    // Optimisations:
    //  4b pointers -XX:+UseCompressedOops if <32Gb RAM
    //  tune DataStructures: http://fastutil.di.unimi.it
    //  convert to IDs and persist (measure size/throughtput)

    //Builds co-occurrence matrix: RDD[ ((i, j), weight)
    val shardedCoocs = SparkPrep.buildCoocuranceMatrix(
      input.map(_.split("\t")),
      cli.window_size(), numShards,
      wordToIdVar
    )

    /* debug output: single shard
    val windowSize = cli.window_size()
    val coocs = SparkPrep
      .wordsToIds(input.map(_.split("\t")), wordToIdVar)
      .flatMap(SparkPrep.generateCoocurance(_, windowSize))

    val shardedCoocs = coocs
      .reduceByKeyLocally(_+_)
      .toSeq.sortBy(_._1)
      .foreach { case ((i, j), w) =>
          println(s"$i $j $w")
      }
    */
    // debug output: before encoding to .pb
    //shardedCoocs.saveAsTextFile(cli.output_dir())

    //Output single tf.train.Example per partition
    val serializedPb = shardedCoocs.mapPartitionsWithIndex( (index, partition) => {
      convertToProtobuf(index, partition, numShards, shardSize)
    }, true)

    serializedPb.saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](cli.output_dir())

    // TODO(bzz): write the sorted vocab to {row, col}_vocab.txt
    saveDebugDict(dict, cli.output_dir())

    //TODO(bzz): output `{row, col}_sums.txt`
  }


  def getContext(sparkMaster: String): (SparkContext, SparkSession) = {
    //conf.registerKryoClasses(Array(classOf[xxxx]))

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master(sparkMaster)
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    (spark.sparkContext, spark)
  }

  /**
    * Given the set of co-occurrence in a single shard, converts them to tf.Example ProtoBuf format
    * Uses https://github.com/tensorflow/ecosystem/tree/master/hadoop#use-with-spark
    *
    * @param index shard number
    * @param partition Iterator over single shard
    * @return
    */
  def convertToProtobuf(index: Int, partition: Iterator[((Int, Int), Double)], numShards: Int, shardSize: Int) = {
      val intListRow = Int64List.newBuilder()
      val intListCol = Int64List.newBuilder()

      //invers of ShardPartitioner.getPartition()
      val row_shard = index % numShards
      val col_shard = index / numShards
      for (i <- 0 to shardSize - 1) {
        intListRow.addValue(row_shard + i * numShards)
        intListCol.addValue(col_shard + i * numShards)
      }

      val intListSparseRow = Int64List.newBuilder()
      val intListSparseCol = Int64List.newBuilder()
      val floatListWeight = FloatList.newBuilder()
      partition.foreach { case ((i, j), w) =>
        intListSparseRow.addValue(i/numShards)
        intListSparseCol.addValue(j/numShards)
        floatListWeight.addValue(w.toFloat) //TODO(bzz): check how we loose accuracy here
      }
      val features = Features.newBuilder()
        .putFeature("global_row", Feature.newBuilder().setInt64List(intListRow.build()).build())
        .putFeature("global_col", Feature.newBuilder().setInt64List(intListCol.build()).build())
        .putFeature("sparse_local_row", Feature.newBuilder().setInt64List(intListSparseRow.build()).build())
        .putFeature("sparse_local_col", Feature.newBuilder().setInt64List(intListSparseCol.build()).build())
        .putFeature("sparse_value", Feature.newBuilder().setFloatList(floatListWeight.build()).build())
        .build()

      val example = Example.newBuilder()
        .setFeatures(features)
        .build()

      Seq((new BytesWritable(example.toByteArray/*.toString.getBytes*/), NullWritable.get())).toIterator
    }

  def saveDebugDict(dict: Seq[(String, Int)], outputDir: String): Unit = {
    new java.io.PrintWriter(outputDir + "/dict_debug.txt") {
      try {
        dict.foreach { case (word, freq) =>
          write(s"$word $freq\n")
        }
      } finally {close}
    }
  }


}