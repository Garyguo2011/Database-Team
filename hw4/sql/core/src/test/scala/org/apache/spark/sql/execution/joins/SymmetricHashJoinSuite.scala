package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, Row, Attribute}
import org.apache.spark.sql.execution.joins.dns.GeneralSymmetricHashJoin
import org.apache.spark.sql.execution.{Record, ComplicatedRecord, PhysicalRDD, SparkPlan}
import org.apache.spark.sql.test.TestSQLContext._
import org.scalatest.FunSuite

import scala.collection.immutable.HashSet

import org.apache.spark.rdd.EmptyRDD

class SymmetricHashJoinSuite extends FunSuite {
  // initialize Spark magic stuff that we don't need to care about
  val sqlContext = new SQLContext(sparkContext)
  val recordAttributes: Seq[Attribute] = ScalaReflection.attributesFor[Record]
  val complicatedAttributes: Seq[Attribute] = ScalaReflection.attributesFor[ComplicatedRecord]

  import sqlContext.createSchemaRDD

  // initialize a SparkPlan that is a sequential scan over a small amount of data
  val smallRDD1: RDD[Record] = sparkContext.parallelize((1 to 100).map(i => Record(i)), 1)
  val smallScan1: SparkPlan = PhysicalRDD(recordAttributes, smallRDD1)
  val smallRDD2: RDD[Record] = sparkContext.parallelize((51 to 150).map(i => Record(i)), 1)
  val smallScan2: SparkPlan = PhysicalRDD(recordAttributes, smallRDD2)

  // the same as above but complicated
  val complicatedRDD1: RDD[ComplicatedRecord] = sparkContext.parallelize((1 to 100).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val complicatedScan1: SparkPlan = PhysicalRDD(complicatedAttributes, complicatedRDD1)
  val complicatedRDD2: RDD[ComplicatedRecord] = sparkContext.parallelize((51 to 150).map(i => ComplicatedRecord(i, i.toString, i*2)), 1)
  val complicatedScan2: SparkPlan = PhysicalRDD(complicatedAttributes, complicatedRDD2)

  val tentenRDD1: RDD[Record] = sparkContext.parallelize((1 to 10).map(i => Record(10)), 1)
  val tentenScan1: SparkPlan = PhysicalRDD(recordAttributes, tentenRDD1)
  val tentenRDD2: RDD[Record] = sparkContext.parallelize((1 to 10).map(i => Record(10)), 1)
  val tentenScan2: SparkPlan = PhysicalRDD(recordAttributes, tentenRDD2)

  val fhvsfdifferentRDD1: RDD[Record] = sparkContext.parallelize((1 to 4).map(i => Record(i)), 1)
  val fhvsfdifferentScan1: SparkPlan = PhysicalRDD(recordAttributes, fhvsfdifferentRDD1)
  val fhvsfdifferentRDD2: RDD[Record] = sparkContext.parallelize((1 to 4).map(i => Record(1)), 1)
  val fhvsfdifferentScan2: SparkPlan = PhysicalRDD(recordAttributes, fhvsfdifferentRDD2)

  val nomatchRDD1: RDD[Record] = sparkContext.parallelize((1 to 4).map(i => Record(i)), 1)
  val nomatchScan1: SparkPlan = PhysicalRDD(recordAttributes, nomatchRDD1)
  val nomatchRDD2: RDD[Record] = sparkContext.parallelize((5 to 8).map(i => Record(i)), 1)
  val nomatchScan2: SparkPlan = PhysicalRDD(recordAttributes, nomatchRDD2)


  

  test ("simple join") {
    val outputRDD = GeneralSymmetricHashJoin(recordAttributes, recordAttributes, smallScan1, smallScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    outputRDD.collect().foreach(x => seenValues = seenValues + x)

    (51 to 100).foreach(x => assert(seenValues.contains(new JoinedRow(Row(x), Row(x)))))
  }

  test ("complicated join") {
    val outputRDD = GeneralSymmetricHashJoin(Seq(complicatedAttributes(0)), Seq(complicatedAttributes(0)), complicatedScan1, complicatedScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    outputRDD.collect().foreach(x => seenValues = seenValues + x)

    (51 to 100).foreach(x => assert(seenValues.contains(new JoinedRow(Row(x, x.toString, x*2), Row(x, x.toString, x*2)))))
  }

  test ("ten ten duplicate join") {
    val outputRDD = GeneralSymmetricHashJoin(recordAttributes, recordAttributes, tentenScan1, tentenScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    outputRDD.collect().foreach(x => seenValues = seenValues + x)

    (1 to 100).foreach(x => assert(seenValues.contains(new JoinedRow(Row(10), Row(10)))))
  }

  test ("four 1 to 1-4 should receive 4 match") {
    val outputRDD = GeneralSymmetricHashJoin(recordAttributes, recordAttributes, fhvsfdifferentScan1, fhvsfdifferentScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    outputRDD.collect().foreach(x => seenValues = seenValues + x)

    (1 to 4).foreach(x => assert(seenValues.contains(new JoinedRow(Row(1), Row(1)))))
  }

  test ("no match 1-4 5-8") {
    val outputRDD = GeneralSymmetricHashJoin(recordAttributes, recordAttributes, nomatchScan1, nomatchScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    outputRDD.collect().foreach(x => seenValues = seenValues + x)
    assert(seenValues.isEmpty)

    // (1 to 4).foreach(x => assert(seenValues.contains(new JoinedRow(Row(1), Row(1)))))
  }

  val diffLengthRDD1: RDD[Record] = sparkContext.parallelize((1 to 4).map(i => Record(i)), 1)
  val diffLengthScan1: SparkPlan = PhysicalRDD(recordAttributes, diffLengthRDD1)
  val diffLengthRDD2: RDD[Record] = sparkContext.parallelize((2 to 10).map(i => Record(i)), 1)
  val diffLengthScan2: SparkPlan = PhysicalRDD(recordAttributes, diffLengthRDD2)

  test ("different size 1-4 2-10") {
    val outputRDD = GeneralSymmetricHashJoin(recordAttributes, recordAttributes, diffLengthScan1, diffLengthScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    outputRDD.collect().foreach(x => seenValues = seenValues + x)
    assert(seenValues.size == 3)

    (2 to 4).foreach(x => assert(seenValues.contains(new JoinedRow(Row(x), Row(x)))))
  }

  val oneEmptyRDD1: RDD[Record] = sparkContext.parallelize((1 to 4).map(i => Record(i)), 1)
  val oneEmptyScan1: SparkPlan = PhysicalRDD(recordAttributes, oneEmptyRDD1)
  // val oneEmptyRDD2: RDD[Record] = sparkContext.parallelize((2 to 10).map(i => Record(i)), 1)
  val oneEmptyRDD2: RDD[Record] = new EmptyRDD(sparkContext)
  val oneEmptyScan2: SparkPlan = PhysicalRDD(recordAttributes, oneEmptyRDD2)

  test ("one empty ") {
    val outputRDD = GeneralSymmetricHashJoin(recordAttributes, recordAttributes, oneEmptyScan1, oneEmptyScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    assert(seenValues.isEmpty)
  }

  val reverseRDD1: RDD[Record] = sparkContext.parallelize((1 to 4).map(i => Record(i)), 1)
  val reverseScan1: SparkPlan = PhysicalRDD(recordAttributes, reverseRDD1)
  val reverseRDD2: RDD[Record] = sparkContext.parallelize((1 to 4).map(i => Record(5-i)), 1)
  val reverseScan2: SparkPlan = PhysicalRDD(recordAttributes, reverseRDD2)

  test ("different size 1-4 4-1") {
    val outputRDD = GeneralSymmetricHashJoin(recordAttributes, recordAttributes, reverseScan1, reverseScan2).execute()
    var seenValues: HashSet[Row] = new HashSet[Row]()

    outputRDD.collect().foreach(x => seenValues = seenValues + x)
    assert(seenValues.size == 4)

    (1 to 4).foreach(x => assert(seenValues.contains(new JoinedRow(Row(x), Row(x)))))
  }

}
