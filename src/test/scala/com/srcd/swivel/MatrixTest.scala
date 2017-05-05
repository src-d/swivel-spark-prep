package com.srcd.swivel

import org.apache.spark.broadcast.Broadcast
import com.holdenkarau.spark.testing.SharedSparkContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.scalatest.Matchers._


@RunWith(classOf[JUnitRunner]) //TODO(bzz): research https://plugins.gradle.org/plugin/com.github.maiflai.scalatesta
class MatrixTest extends FunSuite with SharedSparkContext {

  private val id = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
  private var wordToIdVar: Broadcast[Map[String, Int]] = _

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

  test("Coocs: lines to ids") {
    val expectedIds = Array(Array(1, 2, 3, 1), Array(2, 3, 1));
    // given
    val tokenLines = Array(Array("j","a","b","c","a"), Array("b","c","a")) // j is OOV

    // when
    val ids = SparkPrep.wordsToIds(sc.parallelize(tokenLines), wordToIdVar).collect()

    // then
    assert(ids.length == 2)
    ids should equal (expectedIds)
  }

  test("Coocs: raw lines of tokens to coocurences") {
    val expectedCoocs = Set(
      ((id("a"), id("a")), 0.5),
      ((id("a"), id("b")), 1.0),
      ((id("b"), id("b")), 0.5))
    // given
    val wordWindow = 10
    val tokenLines = Seq(Array("a","b"))

    // when
    val coocs = SparkPrep.buildCoocuranceMatrix(sc.parallelize(tokenLines), wordWindow, wordToIdVar).collect()

    // then
    assert(coocs.length == expectedCoocs.size)
    coocs.toSet should equal (expectedCoocs)
  }

  test("Coocs: 1 line of ids to coocurences") {
    val expectedCoocs = Set(
      ((id("a"), id("a")), 0.5),
      ((id("a"), id("b")), 1.0),
      ((id("b"), id("b")), 0.5))
    // given
    val wordWindow = 10
    val lineIds = Array(id("a"), id("b"))

    // when
    val coocs = SparkPrep.generateCoocurance(lineIds, wordWindow)

    // then
    assert(coocs.size == expectedCoocs.size)
    coocs.toSet should equal (expectedCoocs)
  }

  test("Coocs: 3 line of ids to coocurences") {
    val expectedCoocs = Seq(
      ((id("a"), id("a")), 1.5),
      ((id("a"), id("b")), 3.0),
      ((id("b"), id("b")), 1.5),
      ((id("b"), id("c")), 2.0),
      ((id("c"), id("c")), 1.0),
      ((id("b"), id("d")), 0.5),
      ((id("a"), id("c")), 1.0),
      ((id("c"), id("d")), 1.0),
      ((id("a"), id("d")), 0.3333333333333333),
      ((id("d"), id("d")), 0.5)
    ).toSet

    // given
    val wordWindow = 10

    val linesIds = Seq(
      Array(id("a"), id("b")),
      Array(id("a"), id("b"), id("c")),
      Array(id("a"), id("b"), id("c"), id("d")))

    // when
    val coocs = linesIds
      .flatMap(SparkPrep.generateCoocurance(_, wordWindow))
      .groupBy(_._1).mapValues(_.map(_._2).sum) // for tests only, this happens later in pipeline

    // then
    //assert(coocs.size == expectedCoocs.size)
    coocs.toSet should equal (expectedCoocs)
  }


}
