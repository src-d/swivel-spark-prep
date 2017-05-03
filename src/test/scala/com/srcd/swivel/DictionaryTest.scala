package com.srcd.swivel

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD


@RunWith(classOf[JUnitRunner]) //TODO(bzz): research https://plugins.gradle.org/plugin/com.github.maiflai.scalatesta
class DictionaryTest extends FunSuite with SharedSparkContext {

  test("build histogram") {
    // given
    val tokenLine = Array("d\ta\tb\tc\ta", "b\tc\ta")

    // when
    val words = SparkPrep.buildHistogram(sc.parallelize(tokenLine))


    // then
    assert(words.length == 4)
    assert(words(0) === ("a", 3))
  }

  test("build dictionary: filters minCount") {
    val expected = Array(("a", 4), ("b", 3))
    // given
    val hist = expected ++ Array(("c", 2), ("d", 1), ("e", 0))
    val minCount = 3 //kills c,d,e
    val maxVocab = 10
    val shardSize = 2

    // when
    val vocab = SparkPrep.dictFromHist(hist, minCount, maxVocab, shardSize)

    //then
    assert(vocab === expected)
  }

  test("build dictionary: filters maxVocab") {
    val expected = Array(("a", 4), ("b", 3), ("c", 2))
    // given
    val hist = expected ++ Array(("d", 1), ("e", 0))
    System.out.println(hist)

    val minCount = 1 //kills e
    val maxVocab = 4 //kills d
    val shardSize = 3

    // when
    val vocab = SparkPrep.dictFromHist(hist, minCount, maxVocab, shardSize)

    //then
    assert(vocab === expected)
  }

  test("build dictionary: adjusts up to shard size") {
    val expected = Array(("a", 4), ("b", 3))
    // given
    val hist = expected ++ Array(("c", 2), ("d", 1), ("e", 0))
    System.out.println(hist)

    val minCount = 1  //kills e
    val maxVocab = 3  //kills d
    val shardSize = 2 //kills c

    // when
    val vocab = SparkPrep.dictFromHist(hist, minCount, maxVocab, shardSize)

    //then
    assert(vocab === expected)
  }


}