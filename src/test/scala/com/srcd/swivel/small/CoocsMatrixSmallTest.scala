package com.srcd.swivel.small

import com.srcd.swivel.SparkPrep
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner

/**
  * Co-ocurence generation logic that does not depend on Spark
  */
@RunWith(classOf[JUnitRunner]) //TODO(bzz): research https://plugins.gradle.org/plugin/com.github.maiflai.scalatesta
class CoocsMatrixSmallTest extends FunSuite {

  private val id =  Map("a" -> 0, "b" -> 1, "c" -> 2, "d" -> 3, "e" -> 4)

  test("Coocs: 1 line of IDs to coocurences") {
    val expectedCoocs = Seq(
      ((id("a"), id("a")), 1.0),
      ((id("a"), id("b")), 1.0),
      ((id("b"), id("a")), 1.0),
      ((id("b"), id("b")), 1.0))
    // given
    val wordWindow = 10
    val lineIds = Array(id("a"), id("b"))

    // when
    val coocs = SparkPrep.generateCoocurance(lineIds, wordWindow)

    // then
    assert(coocs.size == expectedCoocs.size)
    coocs.toSeq.sortBy(_._1) should equal (expectedCoocs)
  }

}
