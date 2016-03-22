package org.template.similarproduct

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
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._

import grizzled.slf4j.Logger

case class DataSourceParams(
  appName: String,
  startTime: String,
  untilTime: Option[DateTime]
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
    logger.info(s"Using events since ${startTime}")

    // get all "user" "view" "item" events
    val viewEventsRDD: RDD[ViewEvent] = PEventStore.find(
      appName = dsp.appName,
      startTime = Some(startTime),
      untilTime = dsp.untilTime,
      entityType = Some("user"),
      eventNames = Some(List("view")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc)
      // eventsDb.find() returns RDD[Event]
      .map { event =>
        val viewEvent = try {
          event.event match {
            case "view" => ViewEvent(
              user = event.entityId,
              item = event.targetEntityId.get,
              t = event.eventTime.getMillis)
            case _ => throw new Exception(s"Unexpected event ${event} is read.")
          }
        } catch {
          case e: Exception => {
            logger.error(s"Cannot convert ${event} to ViewEvent." +
              s" Exception: ${e}.")
            throw e
          }
        }
        viewEvent
      }.cache()

    new TrainingData(
      viewEvents = viewEventsRDD
    )
  }
}

case class User()

case class Item(categories: Option[List[String]])

case class ViewEvent(user: String, item: String, t: Long)

class TrainingData(
  val viewEvents: RDD[ViewEvent]
) extends Serializable {
  override def toString = {
    s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)"
  }
}
