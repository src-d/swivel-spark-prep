package tech.sourced.swivel.medium

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.broadcast.Broadcast
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner
import tech.sourced.swivel.SparkPrep


/**
  * Co-ocurence matrix building test, uses Spark in local mode
  *
  * Small  tests are fast, and do not have external dependencies. AKA UnitTests.
  * Medium tests are slower and might have some dependencies.
  * Large  tests are slow, usualy depend on some kind of environement (ext. DB, etc)
  */
@RunWith(classOf[JUnitRunner]) //TODO(bzz): research https://plugins.gradle.org/plugin/com.github.maiflai.scalatesta
class CoocsMatrixMediumTest extends FunSuite with SharedSparkContext {

  private val id = Map("a" -> 0, "b" -> 1, "c" -> 2, "d" -> 3, "e" -> 4)
  private var wordToIdVar: Broadcast[Map[String, Int]] = _
  private val coocsFor3lines = Seq(
      ((id("a"), id("a")), 3.0),
      ((id("a"), id("b")), 3.0),
      ((id("a"), id("c")), 1.0),
      ((id("a"), id("d")), 0.3333333333333333),
      ((id("b"), id("a")), 3.0),
      ((id("b"), id("b")), 3.0),
      ((id("b"), id("c")), 2.0),
      ((id("b"), id("d")), 0.5),
      ((id("c"), id("a")), 1.0),
      ((id("c"), id("b")), 2.0),
      ((id("c"), id("c")), 2.0),
      ((id("c"), id("d")), 1.0),
      ((id("d"), id("a")), 0.3333333333333333),
      ((id("d"), id("b")), 0.5),
      ((id("d"), id("c")), 1.0),
      ((id("d"), id("d")), 1.0))

  override def conf = { //https://issues.apache.org/jira/browse/SPARK-19394
    super.conf.set("spark.driver.host", "localhost")
  }

  override def beforeAll() {
    super.beforeAll()
    wordToIdVar = sc.broadcast(id)
  }

  override def afterAll() {
    super.afterAll()
  }

  test("Coocs: lines of words to IDs") {
    val expectedIds = Array(Array(0, 1, 2, 0), Array(1, 2, 0));
    // given
    val tokenLines = Array(Array("j","a","b","c","a"), Array("b","c","a")) // j is OOV

    // when
    val ids = SparkPrep.wordsToIds(sc.parallelize(tokenLines), wordToIdVar).collect()

    // then
    assert(ids.length == 2)
    ids should equal (expectedIds)
  }

  test("Coocs: raw lines of tokens to concurrences") {
    val numShards = 3
    val expectedCoocs = Seq(
      ((id("a"), id("a")), 2.0),
      ((id("a"), id("b")), 2.0),
      ((id("a"), id("c")), 0.5),
      ((id("b"), id("a")), 2.0),
      ((id("b"), id("b")), 2.0),
      ((id("b"), id("c")), 1.0),
      ((id("c"), id("a")), 0.5),
      ((id("c"), id("b")), 1.0),
      ((id("c"), id("c")), 1.0))
      // merging coocs is related to sharding, so shading impl details leaked here
      .map(toShardIds(numShards, _))

    // given
    val wordWindow = 10
    val tokenLines = Seq(Array("a","b"), Array("a","b","c"))

    // when
    val coocs = SparkPrep.mergeCoocsAndShard2(
      SparkPrep.buildCooccurrenceMatrix(sc.parallelize(tokenLines), wordWindow, wordToIdVar),
      numShards
    ).collect()

    // then
    assert(coocs.length == expectedCoocs.length)
    coocs.sortBy(_._1) should equal (expectedCoocs)
  }

  test("Coocs: 3 line of IDs to coocurences") {
    // given
    val wordWindow = 10

    val linesIds = Seq(
      Array(id("a"), id("b")),
      Array(id("a"), id("b"), id("c")),
      Array(id("a"), id("b"), id("c"), id("d")))

    // when
    val coocs = linesIds
      .flatMap(SparkPrep.generateCooccurrence(_, wordWindow))
      .groupBy(_._1).mapValues(_.map(_._2).sum) // for tests only, this happens later in pipeline

    // then
    assert(coocs.size == coocsFor3lines.size)
    //coocs.toSet should equal (coocsFor3lines.toSet)
    coocs.toSeq.sortBy {_._1} should equal (coocsFor3lines.sortBy {_._1})
  }

  test("Sharding: merge coocs + shard") {
    // given
    val shardSize = 4
    val unMergedCoocsFor3Lines = List(
      ((0,0),1.0), ((0,0),1.0), ((0,0),1.0),
      ((0,1),1.0), ((0,1),1.0), ((0,1),1.0),
      ((0,2),0.5), ((0,2),0.5),
      ((0,3),0.3333333333333333),
      ((1,0),1.0), ((1,0),1.0), ((1,0),1.0),
      ((1,1),1.0), ((1,1),1.0), ((1,1),1.0),
      ((1,2),1.0), ((1,2),1.0),
      ((1,3),0.5),
      ((2,0),0.5),
      ((2,0),0.5),
      ((2,1),1.0), ((2,1),1.0),
      ((2,2),1.0), ((2,2),1.0),
      ((2,3),1.0),
      ((3,0),0.3333333333333333),
      ((3,1),0.5),
      ((3,2),1.0),
      ((3,3),1.0))

    // when
    val shards = SparkPrep.mergeCoocsAndShard2(sc.parallelize(unMergedCoocsFor3Lines), shardSize)

    // then
    assert(shards.getNumPartitions == shardSize*shardSize)
    val numShards = id.size/shardSize
    shards.collect.sortBy(_._1) should equal (coocsFor3lines.sortBy(_._1))
  }

  def toShardIds(numShards: Int, cooc: ((Int,Int), Double)) = {
    val ((i,j),w) = cooc
    ((i,j),w)
  }


}
