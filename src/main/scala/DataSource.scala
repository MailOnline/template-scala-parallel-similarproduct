package org.template.similarproduct

import java.sql.{DriverManager, ResultSet}

import com.github.nscala_time.time.Imports._

import org.joda.time.format.ISODateTimeFormat

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.store.PEventStore
import io.prediction.data._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.storage.StorageLevel._

import grizzled.slf4j.Logger

import org.template.similarproduct.Types._

case class DataSourceParams(
  appName: String,
  startTime: String,
  untilTime: Option[String],
  jdbcUrl: String,
  jdbcUser: String,
  jdbcPass: String,
  jdbcTable: String,
  jdbcPartitions: Option[Long],
  tag: Option[EngineTag]
) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    val dtFormatter =
      ISODateTimeFormat.dateTimeNoMillis().withOffsetParsed()

    val startTime = dtFormatter.parseDateTime(dsp.startTime)
    val untilTime =
      dsp.untilTime.map(dtFormatter.parseDateTime(_)).getOrElse(DateTime.now)

    val partitions =
      scala.math.min(
        new Duration(untilTime.getMillis - startTime.getMillis).getStandardDays,
        dsp.jdbcPartitions.getOrElse(4.toLong))
      .toInt

    val query = s"""
      select entityId, targetEntityId from ${dsp.jdbcTable}
      where eventTime >= to_timestamp(?) and eventTime <= to_timestamp(?)
        and event='view' and entitytype='user' and targetentitytype='item'
        and targetentitytype <> '999999'
      """

    logger.info(s"Using events since ${dsp.startTime} read from ${dsp.jdbcTable} in ${dsp.jdbcPartitions} partitions")
    // get all "user" "view" "item" events
    val viewEventsRDD: RDD[ViewEvent] = new JdbcRDD(
      sc,
      () => DriverManager.getConnection(
        dsp.jdbcUrl, dsp.jdbcUser, dsp.jdbcPass),
      query,
      startTime.getMillis / 1000,
      untilTime.getMillis / 1000,
      partitions,
      (r: ResultSet) => ViewEvent(
        user = r.getString("entityId"),
        item = r.getString("targetEntityId"))).cache()

    val baseTag =
      dtFormatter.print(startTime) + "/" + dtFormatter.print(untilTime)

    val tag: String = dsp.tag match {
      case Some(t) => t
      case None    => baseTag
    }

    new TrainingData(tag, viewEventsRDD)
  }
}

case class User()

case class ViewEvent(user: String, item: Item)

class TrainingData(
  val tag: EngineTag,
  val viewEvents: RDD[ViewEvent]
) extends Serializable {
  override def toString = {
    s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)"
  }
}
