package com.srcd.swivel

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.util.Properties


object SparkPrep {

  def main(args: Array[String]): Unit = {
    //TODO(bzz): CLI args
    //  --input <filename>
    //  --output_dir <directory>
    //  --shard_size <int>
    //  --min_count <int>
    //  --max_vocab <int>
    //  --vocab <filename>
    //  --window_size <int>

    val sparkMaster = Properties.envOrElse("MASTER", "local[*]")
    val (sc, spark) = getContext(sparkMaster)

    //create id->word, and word->id dicts
    val dict = sc.textFile("tokens_10.tsv")
      .flatMap(_.split("\t"))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    dict.collect() foreach println
    // sort
    // drop freq < FLAGS.min_count
    // num_words = min(len(vocab), FLAGS.max_vocab)
    // adjust vocab, so num_words % FLAGS.shard_size == 0
    // keep top num_words

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