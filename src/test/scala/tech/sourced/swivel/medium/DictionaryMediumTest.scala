package tech.sourced.swivel.medium

import com.holdenkarau.spark.testing.SharedSparkContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import tech.sourced.swivel.SparkPrep


@RunWith(classOf[JUnitRunner]) //TODO(bzz): research https://plugins.gradle.org/plugin/com.github.maiflai.scalatesta
class DictionaryMediumTest extends FunSuite with SharedSparkContext {

  override def conf = { //https://issues.apache.org/jira/browse/SPARK-19394
    super.conf.set("spark.driver.host", "localhost")
  }

  test("build histogram") {
    // given
    val tokenLine = Array("d\ta\tb\tc\ta", "b\tc\ta")

    // when
    val words = SparkPrep.buildHistogram(sc.parallelize(tokenLine))


    // then
    assert(words.length == 4)
    assert(words(0) === ("a", 3))
  }

  test("build vocabulary: full word -> id") {
    // given
    val tokenLine = Array("d\ta\tb\tc\ta", "b\tc\ta")

    // when
    val (words, _) = SparkPrep.buildVocab(sc.parallelize(tokenLine), 2, 100, 3)

    // then
    assert(words.size == 3)
    assert(words("a") === 0)
  }

}