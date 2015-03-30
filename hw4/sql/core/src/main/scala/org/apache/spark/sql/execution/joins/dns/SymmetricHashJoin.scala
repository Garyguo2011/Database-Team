package org.apache.spark.sql.execution.joins.dns

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Expression, JoinedRow, Projection}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BuildSide
import org.apache.spark.util.collection.CompactBuffer

import scala.collection.mutable.HashMap

// We add Arraylist
import java.util.{ArrayList => JavaArrayList}
import scala.collection.mutable.Queue


/**
 * ***** TASK 1 ******
 *
 * Symmetric hash join is a join algorithm that is designed primarily for data streams.
 * In this algorithm, you construct a hash table for _both_ sides of the join, not just
 * one side as in regular hash join.
 *
 * We will be implementing a version of symmetric hash join that works as follows:
 * We will start with the "left" table as the inner table and "right" table as the outer table
 * and begin by streaming tuples in from the inner relation. For every tuple we stream in
 * from the inner relation, we insert it into its corresponding hash table. We then check if
 * there are any matching tuples in the other relation -- if there are, then we join this
 * tuple with the corresponding matches. Otherwise, we switch the inner
 * and outer relations -- that is, the old inner becomes the new outer and the old outer
 * becomes the new inner, and we proceed to repeat this algorithm, streaming from the
 * new inner.
 */
trait SymmetricHashJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val rightKeys: Seq[Expression]
  val left: SparkPlan
  val right: SparkPlan

  override def output = left.output ++ right.output

  @transient protected lazy val leftKeyGenerator: Projection =
    newProjection(leftKeys, left.output)

  @transient protected lazy val rightKeyGenerator: Projection =
    newProjection(rightKeys, right.output)

  /**
   * The main logic for symmetric hash join. You do not need to worry about anything other than this method.
   * This method takes in two iterators (one for each of the input relations), and returns an iterator (
   * representing the result of the join).
   *
   * If you find the method definitions provided to be counter-intuitive or constraining, feel free to change them.
   * However, note that if you do:
   *  1. we will have a harder time helping you debug your code.
   *  2. Iterators must implement next and hasNext. If you do not implement those two methods, your code will not compile.
   *
   * **NOTE**: You should return JoinedRows, which take two input rows and returns the concatenation of them.
   * e.g., `new JoinedRow(row1, row2)`
   *
   * @param leftIter an iterator for the left input
   * @param rightIter an iterator for th right input
   * @return iterator for the result of the join
   */
  protected def symmetricHashJoin(leftIter: Iterator[Row], rightIter: Iterator[Row]): Iterator[Row] = {
    new Iterator[Row] {
      /* Remember that Scala does not have any constructors. Whatever code you write here serves as a constructor. */
      // IMPLEMENT ME
      
      var innerRelation = 0
      // 0 represent Left

      val leftHT = new HashMap[Int, JavaArrayList[Row]]()
      val rightHT = new HashMap[Int, JavaArrayList[Row]]()
      var nextMatch: Queue[JoinedRow] = new Queue[JoinedRow]()

      var innerHT = leftHT
      var outerHT = rightHT
      var innerIter: Iterator[Row] = leftIter
      var outerIter: Iterator[Row] = rightIter

      //  leftKeyGenerator(row).hashcode() for hashtable key.
      //  rightKeyGenerator(row).hashcode() for hashtable key.
      /**
       * This method returns the next joined tuple.
       *
       * *** THIS MUST BE IMPLEMENTED FOR THE ITERATOR TRAIT ***
       */
      override def next() = {
        // IMPLEMENT ME
        // println("dequeue...")
        // var tmp = nextMatch.dequeue()
        // print(tmp)
        // tmp
        nextMatch.dequeue()
      }

      /**
       * This method returns whether or not this iterator has any data left to return.
       *
       * *** THIS MUST BE IMPLEMENTED FOR THE ITERATOR TRAIT ***
       */
      override def hasNext() = {
        // IMPLEMENT ME
        if (!nextMatch.isEmpty){
          true
        }else{
          findNextMatch()  
        }
      }

      /**
       * This method is intended to switch the inner & outer relations.
       */
      private def switchRelations() = {
        // IMPLEMENT ME
        if (innerRelation == 0){
          innerIter = rightIter
          outerIter = leftIter
          innerHT = rightHT
          outerHT = leftHT
          innerRelation = 1
        }else{
          innerIter = leftIter
          outerIter = rightIter
          innerHT = leftHT
          outerHT = rightHT
          innerRelation = 0
        }
      }

      /**
       * This method is intended to find the next match and return true if one such match exists.
       *
       * @return whether or not a match was found
       */
      def findNextMatch(): Boolean = {
        // IMPLEMENT ME

        while (innerIter.hasNext || outerIter.hasNext){
          if (innerIter.hasNext){
            var innerRow = innerIter.next()
            // print(innerRow)
            var innerkey = leftKeyGenerator.apply(innerRow).hashCode()
            var outerkey = rightKeyGenerator.apply(innerRow).hashCode()

            if (innerRelation == 0){
              innerkey = leftKeyGenerator.apply(innerRow).hashCode()
              outerkey = rightKeyGenerator.apply(innerRow).hashCode()  
            }else{
              innerkey = rightKeyGenerator.apply(innerRow).hashCode()
              outerkey = leftKeyGenerator.apply(innerRow).hashCode()
            }

            // add to hashtable
            if (innerHT.contains(innerkey)){
              innerHT(innerkey).add(innerRow)
            }else{
              var rowList: JavaArrayList[Row] = new JavaArrayList[Row]()
              rowList.add(innerRow)
              innerHT.put(innerkey, rowList)
            }

            // matching
            if (outerHT.contains(outerkey)){
              for( i <- 0 to rightHT(outerkey).size - 1){
                nextMatch.enqueue(new JoinedRow(innerRow, rightHT(outerkey).get(i)))
                // print("enqueue")
                // println(nextMatch)
              }
              return true
            }else{
              switchRelations()
            }
          }else{
            switchRelations()
          }
        }
        false
      }

    }
  }
}