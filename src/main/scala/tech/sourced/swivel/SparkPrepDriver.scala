package tech.sourced.swivel

import java.io.File
import java.util.{HashMap => JHashMap}

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.tensorflow.example._
import org.tensorflow.hadoop.io.WholeFileOutputFormat

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.Properties


/**
  * Distributed data pre-processing for Swivel algorithm https://arxiv.org/abs/1602.02215
  *
  * Is equivalent to prep.py (and fastprep) from
  * https://github.com/tensorflow/models/tree/master/swivel#preparing-the-data-for-training
  */
object SparkPrep {

  val defaultMinCount = 5
  val defaultMaxVocab = 4096 * 64
  val defaultShardSize = 4096
  val defaultWindowSize = 10
  val defaultOutputDir = "/tmp/swivel-spark"

  /**
   * Histogram:  word -> freq
   * Dictionary: id -> (word, freq) _ordered_ seq of word freq tuples
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
    println("Building vocabulary")
    val hist = SparkPrep.buildHistogram(rdd)
    val dict = SparkPrep.dictFromHist(hist, minCount, maxVocab, shardSize)
    val vocab = SparkPrep.vocabFromDict(dict)
    println(s"Done. ${dict.length} words found")

    (vocab, dict)
  }

  /**
    * Reads existing vocabulary
    * @param vocabFile path to vocabulary file
    */
  def readVocab(vocabFile: String, sc: SparkContext): (Map[String, Int], Seq[(String, Int)]) = {
    val dict = if (sc == null) { //mostly for tests
      println(s"Reading vocabulary from local FS ${vocabFile}")
      Source.fromFile(vocabFile)
        .getLines
        .toList
        .zipWithIndex
    } else {
      println(s"Reading vocabulary from cluster FS ${vocabFile}")
      sc.textFile(vocabFile, 1)
        .collect
        .toList
        .zipWithIndex
    }
    val vocab = dict.toMap
    println(s"Done. ${dict.length} words found")
    (vocab, dict)
  }

  /**
    * Converts array of tokens to Ids
    *
    * @param rdd
    * @param wordToIdVar
    * @return
    */
  def wordsToIds(rdd: RDD[Array[String]], wordToIdVar: Broadcast[Map[String, Int]]): RDD[Array[Int]] = {
    //TODO(bzz): Re-implement using RDD[fastutils.IntArrayList]
    //https://github.com/src-d/swivel-spark-prep/issues/5
    rdd.map(tokens => {
        tokens.flatMap { // flat is important, skips OOV
          wordToIdVar.value.get(_)
        }
      })
  }

  /**
    * Transforms to Id each token, and list elements of co-ocurence matrix.
    * Each matrix element (i,j) can be listed multiple times.
    *
    * @param rdd Tokenized lines of input
    * @param windowSize size of co-occurrence window
    * @param vocab word -> id
    * @return co-occurrence matrix as ((i, j), weight)
    */
  def buildCooccurrenceMatrix(
    rdd: RDD[Array[String]],
    windowSize: Int,
    vocab: Broadcast[Map[String, Int]]
  ): RDD[((Int, Int), Double)] = {

    val ids = wordsToIds(rdd, vocab)
    val coocs = ids.flatMap(generateCooccurrence(_, windowSize))
    coocs
  }

  def generateCooccurrence(ids: Array[Int], windowSize: Int) = {
    //TODO(bzz): Re-implement using fastutils.ObjectIntMap
    //https://github.com/src-d/swivel-spark-prep/issues/5
    val coocs = mutable.HashMap[(Int, Int), Double]().withDefaultValue(0)
    0 until ids.length foreach { pos =>
      val lid = ids(pos)
      val windowExtent = Math.min(windowSize + 1, ids.length - pos)
      1 until windowExtent foreach { off =>
        val rid = ids(pos + off)
        val count = 1.0 / off
        val pair = (Math.min(lid, rid), Math.max(lid, rid))
        coocs(pair) += count
        coocs(pair.swap) += count
      }
      val pair = (lid, lid)
      coocs(pair) += 1.0  // Add 1 (not 1/2 as before) since we do not output (a, b) and (b, a) separately
    }
    coocs
  }

  /**
    * For given co-occurrence matrix, shards it to <bold>numShards * numShards</bold> pices
    *
    * @param coocs
    * @param numShards number of shards along the dimension
    * @return
    */
  def mergeCoocsAndShard(coocs: RDD[((Int, Int), Double)], numShards: Int): RDD[((Int, Int), Double)] = {
    val shardedCoocs = coocs
      .reduceByKey(new ShardPartitioner(numShards), _+_)
      .mapPartitions(_.toSeq.sortBy(_._1).toIterator, true)
    shardedCoocs
  }

  /**
    * This is equivalent of above, but
    *  - leveraging efficient sort in repartitionAndSortWithinPartitions()
    *  - manual map-side aggregate, taking advantage of partitions being sorted
    */
  def mergeCoocsAndShard2(coocs: RDD[((Int, Int), Double)], numShards: Int): RDD[((Int, Int), Double)] = {
    val shardedCoocs = coocs
      .repartitionAndSortWithinPartitions(new ShardPartitioner(numShards))
      .mapPartitionsWithIndex( (i, entries) => {
        val map = new JHashMap[(Int, Int), Double]
        var curEntry = entries.next()
        var acc = curEntry._2
        if (!entries.hasNext) {
          map.put(curEntry._1, acc)
        }
        while (entries.hasNext) {
          val nextEntry = entries.next()
          if (curEntry._1 == nextEntry._1) {
            acc += nextEntry._2
            if (!entries.hasNext) {
              map.put(curEntry._1, acc)
            }
          } else {
            map.put(curEntry._1, acc)
            curEntry = nextEntry
            acc = nextEntry._2
          }
        }
        map.asScala.iterator
      }, true)
    shardedCoocs
  }

  /**
    * This is another equivalent of the above, combining both techniques
    */
  def mergeCoocsAndShard3(coocs: RDD[((Int, Int), Double)], numShards: Int): RDD[((Int, Int), Double)] = {
    coocs
      .repartitionAndSortWithinPartitions(new ShardPartitioner(numShards))
      .mapPartitions(_.toSeq.sortBy(_._1).toIterator, true)
    coocs
  }

  /**
  * Used for debug output
  */
  def mergeCoocsInSingleShard(rdd: RDD[((Int, Int), Double)]) = rdd
    .reduceByKeyLocally(_+_)
    .toSeq.sortBy(_._1)
    .foreach { case ((i, j), w) =>
      println(s"$i $j $w")
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

    //inverse of ShardPartitioner.getPartition()
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
      intListSparseRow.addValue(i / numShards)
      intListSparseCol.addValue(j / numShards)
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

}


/**
  * Custom Partitioner to split co-occurrence matrix to smaller shards
  *
  * Number of partitions is square the number of shards.
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
//TODO(bzz): add companion object and extract


object SparkPrepDriver {

  def main(args: Array[String]): Unit = {
    val cli = new Cli(args)
    if (args.length < 1) {
      cli.printHelp()
      System.exit(1)
    }
    cli.verify()

    val sparkMaster = Properties.envOrElse("MASTER", "local[*]")
    val (sc, spark) = getContext(sparkMaster)

    val input = sc.textFile(cli.input())

    // Create word->id map
    val (wordToId, dict) = if (cli.vocab().isEmpty) {
      SparkPrep.buildVocab(input, cli.minCount(), cli.maxVocab(), cli.shardSize())
    } else {
      SparkPrep.readVocab(cli.vocab(), sc)
    }

    val wordToIdVar = sc.broadcast(wordToId)
    val numShards = dict.size / cli.shardSize()
    print(s"Breaking matrix to $numShards shards")
    val shardSize = cli.shardSize()

    // Builds co-occurrence matrix: RDD[ ((i, j), weight)
    val coocs = SparkPrep.buildCooccurrenceMatrix(
      input.map(_.split("\t")),
      cli.windowSize(),
      wordToIdVar
    )
    val shardedCoocs = SparkPrep.mergeCoocsAndShard2(coocs, numShards)
    shardedCoocs.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //Output single tf.train.Example per partition
    val serializedPb = shardedCoocs.mapPartitionsWithIndex( (index, partition) => {
      SparkPrep.convertToProtobuf(index, partition, numShards, shardSize)
    }, true)

    serializedPb.saveAsNewAPIHadoopFile[WholeFileOutputFormat](cli.outputDir())

    writeRowColDict(sc.parallelize(dict, 1), cli.outputDir())
    writeAndCountRowColSums(shardedCoocs, cli.outputDir(), sc)
  }

  /**
    * Writes row and column dictionaries to `{row, col}_vocab.txt`
    *
    * @param dict
    * @param outputDir
    */
  def writeRowColDict(dict: RDD[(String, Int)], outputDir: String, debug: Boolean = false) = {
    writeDictToHdfs(dict, outputDir, "row_vocab.txt", debug)
    writeDictToHdfs(dict, outputDir, "col_vocab.txt", debug)
  }

  def writeDictToLocalFs(dict: Seq[(String, Int)], outputDir: String, fileName: String, debug: Boolean) = {
    val path = outputDir :: fileName :: Nil mkString File.separator
    println(s"Writing dict to $path")
    new java.io.PrintWriter(path) {
      try {
        dict.foreach { case (word, freq) =>
          if (debug) {
            write(s"$word $freq\n")
          } else {
            write(s"$word\n")
          }
        }
      } finally {close}
    }
  }

  def writeDictToHdfs(dict: RDD[(String, Int)], outputDir: String, fileName: String, debug: Boolean) = {
    val path = outputDir :: fileName :: Nil mkString File.separator
    dict.map(_._1).saveAsTextFile(path)
  }

  /**
    * Counts and writes marginal row and column sums to `{row, col}_sums.txt`
    *
    * @param coocs the co-occurrence matrix
    */
  def writeAndCountRowColSums(coocs: RDD[((Int, Int), Double)], outputDir: String, sc: SparkContext) = {
    val sum = coocs
      .flatMap { case ((i, j), w) =>
        Seq((i, w), (j, w))
      }
      .reduceByKey(_ + _)
      .collect() //vocabulary fits in memory, so avoid extra shuffle and sort on driver
      .sortBy(_._1)

    val pSum = sc.parallelize(sum, 1)

    writeSumToHdfs(pSum, outputDir, "row_sums.txt")
    writeSumToHdfs(pSum, outputDir, "col_sums.txt")
  }

  def writeSumToLocalFs(sum: Array[(Int, Double)], outputDir: String, fileName: String) = {
    val path = outputDir :: fileName :: Nil mkString File.separator
    println(s"Writing sum to $path")
    new java.io.PrintWriter(path) {
      try {
        sum.foreach { case (_, w) =>
          write(s"${w/2}\n")
        }
      } finally { close }
    }
  }

  def writeSumToHdfs(dict: RDD[(Int, Double)], outputDir: String, fileName: String) = {
    val path = outputDir :: fileName :: Nil mkString File.separator
    dict.map(_._2 / 2).saveAsTextFile(path)
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

}