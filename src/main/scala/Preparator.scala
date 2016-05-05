package org.template.similarproduct

import io.prediction.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    new PreparedData(trainingData.tag, trainingData.itemRatings)
  }
}

class PreparedData(
  val tag: Types.EngineTag,
  val itemRatings: RDD[ItemRating]
) extends Serializable
