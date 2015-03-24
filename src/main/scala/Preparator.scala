package org.template.classification

import java.util
import java.util.Scanner

import edu.stanford.nlp.process.Morphology
import io.prediction.controller.PPreparator
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD


class PreparedData(
  val labeledPoints: RDD[LabeledPoint]
) extends Serializable

class Preparator extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {

    val dictionary = new util.ArrayList[String]()



    new PreparedData(trainingData.labeledPoints.)
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
