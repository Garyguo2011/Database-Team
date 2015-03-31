package org.apache.spark.sql.execution.joins.dns

import java.util.{HashMap => JavaHashMap, ArrayList => JavaArrayList}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, Projection, Expression}
import org.apache.spark.sql.execution.SparkPlan

// we import
import scala.collection.mutable.Queue
import collection.JavaConversions._

/**
 * In this join, we are going to implement an algorithm similar to symmetric hash join.
 * However, instead of being provided with two input relations, we are instead going to
 * be using a single dataset and obtaining the other data remotely -- in this case by
 * asynchronous HTTP requests.
 *
 * The dataset that we are going to focus on reverse DNS, latitude-longitude lookups.
 * That is, given an IP address, we are going to try to obtain the geographical
 * location of that IP address. For this end, we are going to use a service called
 * telize.com, the owner of which has graciously allowed us to bang on his system.
 *
 * For that end, we have provided a simple library that makes asynchronously makes
 * requests to telize.com and handles the responses for you. You should read the
 * documentation and method signatures in DNSLookup.scala closely before jumping into
 * implementing this.
 *
 * The algorithm will work as follows:
 * We are going to be a bounded request buffer -- that is, we can only have a certain number
 * of unanswered requests at a certain time. When we initialize our join algorithm, we
 * start out by filling up our request buffer. On a call to next(), you should take all
 * the responses we have received so far and materialize the results of the join with those
 * responses and return those responses, until you run out of them. You then materialize
 * the next batch of joined responses until there are no more input tuples, there are no
 * outstanding requests, and there are no remaining materialized rows.
 *
 */
trait DNSJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val left: SparkPlan 

  override def output = left.output

  @transient protected lazy val leftKeyGenerator: Projection =
    newProjection(leftKeys, left.output)

  // How many outstanding requests we can have at once.
  val requestBufferSize: Int = 300

  /**
   * The main logic for DNS join. You do not need to implement anything outside of this method.
   * This method takes in an input iterator of IP addresses and returns a joined row with the location
   * data for each IP address.
   *
   * If you find the method definitions provided to be counter-intuitive or constraining, feel free to change them.
   * However, note that if you do:
   *  1. we will have a harder time helping you debug your code.
   *  2. Iterators must implement next and hasNext. If you do not implement those two methods, your code will not compile.
   *
   * **NOTE**: You should return JoinedRows, which take two input rows and returns the concatenation of them.
   * e.g., `new JoinedRow(row1, row2)`
   *
   * @param input the input iterator
   * @return the result of the join
   */
  def hashJoin(input: Iterator[Row]): Iterator[Row] = {
    new Iterator[Row] {
      // IMPLEMENT ME
      val requestHT = new ConcurrentHashMap[Int, Row]()
      val responseHT = new ConcurrentHashMap[Int, Row]()
      // val bufferLookup = new ConcurrentHashMap[Row, Int]()
      val helperHT = new ConcurrentHashMap[Row, Int]()

      var nextMatch: Queue[JoinedRow] = new Queue[JoinedRow]()
      var requestNum = 0

      var curNum = 0
      var curIP: String = ""

      while (input.hasNext && requestHT.size < requestBufferSize){
        requestHT put(requestNum, input.next())
        requestNum += 1
      }

      /**
       * This method returns the next joined tuple.
       *
       * *** THIS MUST BE IMPLEMENTED FOR THE ITERATOR TRAIT ***
       */
      override def next() = {
        // IMPLEMENT ME

        // Initialize - Fill in the request buffer until the first response
        // while (input.hasNext && requestHT.size < requestBufferSize && responseHT.isEmpty){
          // var requestRow = input.next()

        // }

        // if (!nextMatch.isEmpty){
        //   return nextMatch.dequeue()
        // }else{
        //   for ((k, v) <- requestHT){
        //     if (responseHT.contains(k)){
        //       nextMatch.enqueue(new JoinedRow(requestHT(k), responseHT(k)))
        //       bufferLookup put(requestNum, requestRow)
        //       requestHT remove(requestNumber)
        //       var requestMaked = false
        //       while (input.hasNext && requestHT.size() < requestBufferSize && !requestMaked){
        //         var fetchRequestRow = input.next()
        //         if bufferLookup.contains(fetchRequestRow){
        //           nextMatch.enqueue(new JoinedRow(fetchRequestRow, responseHT(bufferLookup(fetchRequestRow))))
        //         }else{
        //           requestHT put(requestNum, fetchRequestRow)
        //           requestIP = requestRow.getString(0)
        //           makeRequest()
        //           requestNum += 1
        //           requestMaked = true
        //         }

        //       }
        //     }
        //   }
        // }


        while(nextMatch.isEmpty){
          // Respont HT
          println(responseHT)
          for ((k, v) <- responseHT){
            if (helperHT.contains(v) && requestHT.contains(helperHT(v))){
              nextMatch.enqueue(new JoinedRow(v, responseHT(helperHT(v))))
              // bufferLookup put(requestNum, requestRow)
              // Consider when add to cache
              requestHT remove(requestNum)
              if (input.hasNext){
                var fetchRequestRow = input.next()
                requestHT put(requestNum, fetchRequestRow)
                requestNum += 1
              }
            }
          }
              
          // Request HT
          for ((qk, qv) <- requestHT){
            if (responseHT.contains(qk)){
              nextMatch.enqueue(new JoinedRow(requestHT(qk), responseHT(qk)))
              println("NOT HERE")
              // delete?
              requestHT remove(qk)
              if (input.hasNext){
                var fetchRequestRow = input.next()
                requestHT put (requestNum, fetchRequestRow)
                requestNum += 1
              }
            }else{
             if (!helperHT.contains(qv)){
                curNum = qk
                curIP = requestHT(qk).getString(0)
                println("requesting... " + curNum)
                makeRequest()
                helperHT put(qv, curNum)
              }
            }
          }
        }

        println ("In NExt....")
        println (nextMatch)

        nextMatch.dequeue()

        //       var requestMaked = false
        //       while (input.hasNext && requestHT.size() < requestBufferSize && !requestMaked){
        //         var fetchRequestRow = input.next()
        //         if bufferLookup.contains(fetchRequestRow){
        //           nextMatch.enqueue(new JoinedRow(fetchRequestRow, responseHT(bufferLookup(fetchRequestRow))))
        //         }else{
        //           requestHT put(requestNum, fetchRequestRow)
        //           requestIP = requestRow.getString(0)
        //           makeRequest()
        //           requestNum += 1
        //           requestMaked = true
        //         }

        //       }
        //     }
        //   }
        // }

            
        
        // // var outerkey = rightKeyGenerator.apply(innerRow).hashCode()
        // while (requestHT.isEmpty()){
        //   var requestRow = input.next()
        //   // requestIP = requestRow.getString(0)
        //   // var ipKey = leftKeyGenerator.apply(requestRow).hashCode()
        //   if bufferLookup.contains(requestRow){
        //     nextMatch.enqueue(new JoinedRow(requestRow, responseHT(bufferLookup(requestRow))))
        //   }else{
            

        //   if (!nextMatch.isEmpty){
        //     return nextMatch.dequeue()
        //   }

        //     requestHT put(requestNum, requestRow)
        //     makeRequest()
        //     requestNum += 1

        //     if

        //   }

        //   if (!nextMatch.isEmpty()){
        //     return nextMatch.dequeue()
        //   }

        // }
        // return n
        


        
        // if (responseHT.contains)

      }

      /**
       * This method returns whether or not this iterator has any data left to return.
       *
       * *** THIS MUST BE IMPLEMENTED FOR THE ITERATOR TRAIT ***
       */
      override def hasNext() = {
        // IMPLEMENT ME
        println ("1")
        input.hasNext || !nextMatch.isEmpty || !requestHT.isEmpty
      }


      /**
       * This method takes the next element in the input iterator and makes an asynchronous request for it.
       */
      private def makeRequest() = {
        // IMPLEMENT ME
        DNSLookup.lookup(curNum, curIP, responseHT, requestHT)
      }
    }
  }
}