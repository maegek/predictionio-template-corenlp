package org.template.classification

import java.util
import java.util.Scanner

import edu.stanford.nlp.process.Morphology
import grizzled.slf4j.Logger
import io.prediction.controller.{EmptyActualResult, EmptyEvaluationInfo, PDataSource, Params}
import io.prediction.data.storage.{PEvents, Storage}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable

case class DataSourceParams(appId: Int) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]


  override
  def readTraining(sc: SparkContext): TrainingData = {
    val eventsDb = Storage.getPEvents()

    val dictionary = new mutable.ArrayBuffer[String]()
    val ads2words2counts = new mutable.HashMap[String, mutable.Map[String, Int]]
    val numAds = getWordList(eventsDb, sc, dictionary, ads2words2counts)
    val indexMap = getIndexMap(dictionary)











    val labeledPoints: RDD[LabeledPoint] = eventsDb.aggregateProperties(
      appId = dsp.appId,
      entityType = "user",
      // only keep entities with these required properties defined
      required = Some(List("Category", "Description")))(sc)
      // aggregateProperties() returns RDD pair of
      // entity ID and its aggregated properties
      .map { case (entityId, properties) =>
        try {
          LabeledPoint(properties.get[Int]("Category"),
              properties.get("Description")
          )
        } catch {
          case e: Exception => {
            logger.error(s"Failed to get properties ${properties} of" +
              s" ${entityId}. Exception: ${e}.")
            throw e
          }
        }
      }

    new TrainingData(labeledPoints)
  }

  def getIndexMap(dictionary: mutable.ArrayBuffer[String]): mutable.HashMap[String, Int] = {
    val result = new mutable.HashMap[String, Int]()
    for (i <- 0 to dictionary.length) {
      result += (dictionary(i) -> i)
    }
    result
  }

  // Fills the given ArrayBuffer with a sorted list of all words
  // returns the number of ads in the eventsDb
  private def getWordList(eventsDb: PEvents, sc: SparkContext,
                          dictionary: mutable.ArrayBuffer[String],
                          adId2words2counts: mutable.Map[Int,
                            mutable.HashMap[String, Int]]): Int = {
    val allWords = new mutable.HashSet[String]
    var numAds = 0


    // Most of this is just a hack to iterate through all of the data
    // The relevant part is marked
    eventsDb.aggregateProperties(
      appId = dsp.appId,
      entityType = "user",
      required = Some(List("Category", "Description")))(sc)
      .map { case (entityId, properties) =>
      try {
        LabeledPoint(properties.get[Int]("Category"),
          properties.get("Description")
        )

        // This is the relavent part. I add all of the words into the Set,
        // count up the number of adds, and map
        // we will use numAds as an id for each ad
        numAds += 1
        adId2words2counts += (numAds -> new mutable.HashMap[String, Int]())

        val tokens = tokenize(properties.get("Description"))
        for (token <- tokens) {
          if (!allWords.contains(token)) {
            allWords.add(token)
            dictionary += token
          }
          if (!adId2words2counts(numAds).contains(token)) {
            adId2words2counts(numAds) += (token -> 1)
          }
          adId2words2counts(numAds)(token) += 1
        }

      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" ${entityId}. Exception: ${e}.")
          throw e
        }
      }
    }


    dictionary.sortWith(_.compareTo(_) < 0)
    numAds

  }

  // returns a list of the words from the given string after they have been stemmed
  private def tokenize(content: String): util.List[String] = {
    val tReader = new Scanner(content)
    val stemmer = new Morphology()

    val stemmedWords = new util.ArrayList[String]()
    while (tReader hasNext) {
      val word = tReader.next()
      val stemmedWord = stemmer.stem(word)
      stemmedWords.add(stemmedWord)

    }
    stemmedWords
  }


}

class TrainingData(
  val labeledPoints: RDD[LabeledPoint]
) extends Serializable


