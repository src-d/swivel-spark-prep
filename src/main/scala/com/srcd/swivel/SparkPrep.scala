package com.srcd.swivel

import scala.util.Properties


object SparkPrep {

  def main(args: Array[String]): Unit = {
    //input, output paths
    val sparkMaster = Properties.envOrElse("MASTER", "local[*]")
    val (sc, sqlContext) = getContexts(sparkMaster)

    //create id->word, and word->id dicts

    // Optimisations
    //-XX:+UseCompressedOops if <32Gb RAM
    //http://fastutil.di.unimi.it

    //broadcast id->word, and word->id dicts
    //word -> ids



  }


  def getContexts(sparkMaster: String): (SparkContext, SQLContext) = {
    val conf = new SparkConf().
      setMaster(sparkMaster).
      setAppName(this.getClass.getSimpleName)

    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.registerKryoClasses(Array(classOf[xxxx]))

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    (sc, sqlContext)
  }

}