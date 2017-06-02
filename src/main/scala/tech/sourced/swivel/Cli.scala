package com.srcd.swivel

import org.rogach.scallop.ScallopConf


/**
  * Command Line Interface definition
  *
  * Mimics https://github.com/tensorflow/models/tree/master/swivel#preparing-the-data-for-training
  */
class Cli(arguments: Seq[String]) extends ScallopConf(arguments) {
  version("swivel-spark-prep 0.0.1 by Source{d}")
  banner("""Usage: swivel-spark-prep [OPTION]...
           |Swivel-spark-prep parallelize data pre-processing for Swivel ML model
           |Options:
           |""".stripMargin)
  footer("\nFor the details , consult https://github.com/tensorflow/models/tree/master/swivel#preparing-the-data-for-training")

  val input = opt[String](name="input", noshort=true, required = true)
  val outputDir = opt[String](name="output_dir", noshort=true, default = Some(SparkPrep.defaultOutputDir))
  val shardSize = opt[Int](name="shard_size", noshort=true, default = Some(SparkPrep.defaultShardSize))
  val minCount = opt[Int](name="min_count", noshort=true, default = Some(SparkPrep.defaultMinCount))
  val maxVocab = opt[Int](name="max_vocab", noshort=true, default = Some(SparkPrep.defaultMaxVocab))
  val windowSize = opt[Int](name="window_size", noshort=true, default = Some(SparkPrep.defaultWindowSize))
  val vocab = opt[String](name="vocab", noshort=true, default = Some(""))
}
