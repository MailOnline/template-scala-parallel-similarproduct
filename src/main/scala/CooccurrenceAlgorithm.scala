package org.template.similarproduct

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params
import io.prediction.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.template.similarproduct.Types._

case class CooccurrenceAlgorithmParams(
  n: Int // top co-occurrence
) extends Params

class CooccurrenceModel(
  val tag: Types.EngineTag,
  val topCooccurrences: Map[Int, Array[(Int, Int)]],
  val itemStringIntMap: BiMap[String, Int]
) extends Serializable {
  @transient lazy val itemIntStringMap = itemStringIntMap.inverse

  override def toString(): String = {
    s"tag: ${tag}" +
    s" topCooccurrences: [${topCooccurrences.size}]" +
    s"(${topCooccurrences.take(2).toList}...)" +
    s" itemStringIntMap: [${itemStringIntMap.size}]" +
    s"(${itemStringIntMap.take(2).toString}...)"
  }
}

class CooccurrenceAlgorithm(val ap: CooccurrenceAlgorithmParams)
  extends P2LAlgorithm[PreparedData, CooccurrenceModel, Query, PredictedResult] {

  def train(sc: SparkContext, data: PreparedData): CooccurrenceModel = {

    val itemStringIntMap = BiMap.stringInt(data.viewEvents.map(_.item))

    val topCooccurrences = trainCooccurrence(
      events = data.viewEvents,
      n = ap.n,
      itemStringIntMap = itemStringIntMap
    )

    new CooccurrenceModel(
      tag = data.tag,
      topCooccurrences = topCooccurrences,
      itemStringIntMap = itemStringIntMap
    )

  }

  /* given the user-item events, find out top n co-occurrence pair for each item */
  def trainCooccurrence(
    events: RDD[ViewEvent],
    n: Int,
    itemStringIntMap: BiMap[String, Int]): Map[Int, Array[(Int, Int)]] = {

    val userItem = events
      // map item from string to integer index
      .flatMap {
        case ViewEvent(user, item) if itemStringIntMap.contains(item) =>
          Some(user, itemStringIntMap(item))
        case _ => None
      }
      // if user view same item multiple times, only count as once
      .distinct()
      .cache()

    val cooccurrences: RDD[((Int, Int), Int)] = userItem.join(userItem)
      // remove duplicate pair in reversed order for each user. eg. (a,b) vs. (b,a)
      .filter { case (user, (item1, item2)) => item1 < item2 }
      .map { case (user, (item1, item2)) => ((item1, item2), 1) }
      .reduceByKey{ (a: Int, b: Int) => a + b }

    val topCooccurrences = cooccurrences
      .flatMap{ case (pair, count) =>
        Seq((pair._1, (pair._2, count)), (pair._2, (pair._1, count)))
      }
      .groupByKey
      .map { case (item, itemCounts) =>
        (item, itemCounts.toArray.sortBy(_._2)(Ordering.Int.reverse).take(n))
      }
      .collectAsMap.toMap

    topCooccurrences
  }

  def predict(model: CooccurrenceModel, query: Query): PredictedResult = {

    // convert items to Int index
    val queryList: Set[Int] = query.items
      .flatMap(model.itemStringIntMap.get(_))
      .toSet

    val whiteList: Option[Set[Int]] = query.whiteList.map( set =>
      set.map(model.itemStringIntMap.get(_)).flatten
    )

    val blackList: Option[Set[Int]] = query.blackList.map ( set =>
      set.map(model.itemStringIntMap.get(_)).flatten
    )

    val counts: Array[(Int, Int)] = queryList.toVector
      .flatMap { q =>
        model.topCooccurrences.getOrElse(q, Array())
      }
      .groupBy { case (index, count) => index }
      .map { case (index, indexCounts) => (index, indexCounts.map(_._2).sum) }
      .toArray

    val itemScores = counts
      .filter { case (i, v) =>
        isCandidateItem(
          i = i,
          categories = query.categories,
          queryList = queryList,
          whiteList = whiteList,
          blackList = blackList
        )
      }
      .sortBy(_._2)(Ordering.Int.reverse)
      .take(query.num)
      .map { case (index, count) =>
        ItemScore(
          item = model.itemIntStringMap(index),
          score = count
        )
      }

    new PredictedResult(model.tag, itemScores)

  }

  private
  def isCandidateItem(
    i: Int,
    categories: Option[Set[String]],
    queryList: Set[Int],
    whiteList: Option[Set[Int]],
    blackList: Option[Set[Int]]
  ): Boolean = {
    whiteList.map(_.contains(i)).getOrElse(true) &&
    blackList.map(!_.contains(i)).getOrElse(true) &&
    // discard items in query as well
    (!queryList.contains(i))
  }

}
