package com.srcd.swivel

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD


@RunWith(classOf[JUnitRunner]) //TODO(bzz): research https://plugins.gradle.org/plugin/com.github.maiflai.scalatesta
class DictionaryTest extends FunSuite with SharedSparkContext {

  test("dictionary") {
    // given
    val tokenLine = Array("d\ta\tb\tc\ta", "b\tc\ta")

    // when
    val words = SparkPrep.buildDict(sc.parallelize(tokenLine))


    // then
    assert(words.length == 4)
    assert(words(0) === ("a", 3))
  }

}