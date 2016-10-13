import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint



object naive extends App 
{

    val SparkCtx = new SparkContext()

    val dataFile = "DigitalBreathTestData2013_trans.csv"

    val csvData = SparkCtx.textFile(dataFile)

    // CSV데이터를 LabeledPoint 객체로 변환하여 Array에 저장
    val ArrayData = csvData.map{
        csvLine =>
            val colData = csvLine.split(',')
            LabeledPoint(colData(0).toDouble, Vectors.dense(colData(1).split(' ').map(_.toDouble)))
    }

    // Array를 랜덤으로 정답지데이터(7):예측할데이터(3) 비율로 쪼개 
    val Array(trainSet,testSet) = ArrayData.randomSplit(Array(0.7,0.3), seed= 13L)

    // 정답지 학습
    val nbTrained = NaiveBayes.train(trainSet)

    // 테스트셋의 정보로 성별을 예측한 RDD 반환
    val nbPredict = nbTrained.predict(testSet.map(_.features))

    // (예측한 성별,실제 성별) 튜플 리스트 반환
    val predictionAndLabel = nbPredict.zip(testSet.map(_.label))

    // 모델의 정확성 판단
    val accuracy = 100.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testSet.count()

    println("Accuracy : " + accuracy)


}
