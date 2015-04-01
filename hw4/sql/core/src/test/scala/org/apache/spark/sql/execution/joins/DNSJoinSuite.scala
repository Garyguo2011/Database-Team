package org.apache.spark.sql.execution.joins

import java.util.{ArrayList => JavaArrayList}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.joins.dns.GeneralDNSJoin
import org.apache.spark.sql.execution.{PhysicalRDD, SparkPlan}
import org.apache.spark.sql.test.TestSQLContext._
import org.scalatest.FunSuite

import scala.util.Random

import org.apache.spark.rdd.EmptyRDD

case class IP (ip: String)

class DNSJoinSuite extends FunSuite {
  val random: Random = new Random

  // initialize Spark magic stuff that we don't need to care about
  val sqlContext = new SQLContext(sparkContext)
  val IPAttributes: Seq[Attribute] = ScalaReflection.attributesFor[IP]

  var createdIPs: JavaArrayList[IP] = new JavaArrayList[IP]()

  import sqlContext.createSchemaRDD

  // initialize a SparkPlan that is a sequential scan over a small amount of data
  val smallRDD1: RDD[IP] = sparkContext.parallelize((1 to 100).map(i => {
    val ip: IP = IP((random.nextInt(220) + 1) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256))
    createdIPs.add(ip)
    ip
  }), 1)
  val smallScan1: SparkPlan = PhysicalRDD(IPAttributes, smallRDD1)

  test ("simple dns join") {
    val outputRDD = GeneralDNSJoin(IPAttributes, IPAttributes, smallScan1, smallScan1).execute()

    val result = outputRDD.collect
    assert(result.length == 100)

    result.foreach(x => {
      val ip = IP(x.getString(0))
      assert(createdIPs contains ip)
      createdIPs remove ip
    })
  }

  // greater then buffer size
  val largeRDD1: RDD[IP] = sparkContext.parallelize((1 to 350).map(i => {
    val ip: IP = IP((random.nextInt(220) + 1) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256))
    createdIPs.add(ip)
    ip
  }), 1)
  val largeScan1: SparkPlan = PhysicalRDD(IPAttributes, largeRDD1)

  test ("large test dns join") {
    val outputRDD = GeneralDNSJoin(IPAttributes, IPAttributes, largeScan1, largeScan1).execute()

    val result = outputRDD.collect
    assert(result.length == 350)

    result.foreach(x => {
      val ip = IP(x.getString(0))
      assert(createdIPs contains ip)
      createdIPs remove ip
    })
  }

  // Duplication
  val duplicateRDD1: RDD[IP] = sparkContext.parallelize((1 to 10).map(i => {
    val ip: IP = IP((221) + "." + 1 + "." + 1 + "." + 1)
    createdIPs.add(ip)
    ip
  }), 1)
  val duplicateScan1: SparkPlan = PhysicalRDD(IPAttributes, duplicateRDD1)

  test ("duplicate dns join") {
    val outputRDD = GeneralDNSJoin(IPAttributes, IPAttributes, duplicateScan1, duplicateScan1).execute()

    val result = outputRDD.collect
    assert(result.length == 10)

    result.foreach(x => {
      val ip = IP(x.getString(0))
      assert(createdIPs contains ip)
      createdIPs remove ip
    })
  }

  // empty
  val emptyRDD1: RDD[IP] = new EmptyRDD(sparkContext)
  val emptyScan1: SparkPlan = PhysicalRDD(IPAttributes, emptyRDD1)

  test ("empty dns join") {
    val outputRDD = GeneralDNSJoin(IPAttributes, IPAttributes, emptyScan1, emptyScan1).execute()

    val result = outputRDD.collect
    assert(result.length == 0)
  }


}
