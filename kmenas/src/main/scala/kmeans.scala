import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans,KMeansModel}



object kmeans extends App 
{

    val SparkCtx = new SparkContext()

    val dataFile = "DigitalBreathTestData2013_kmeans.csv"

    val csvData = SparkCtx.textFile(dataFile)

    // CSV데이터를 Vector 객체로 변환하여 Array에 저장
    val VectorData = csvData.map{
        csvLine =>
            Vectors.dense( csvLine.split(',').map(_.toDouble))
    }


    // 클러스터 갯수와 반복횟수 정의
    val numClusters = 3
    val MaxIterations = 50


    // 아래 두 값은 디폴트값이므로 설정을 안해줘도 된다.
    // K-Means알고리즘은 초기값을 어떻게 선택하느냐에 따라 성능이 달라진다.RANDOM, K_MEANS_PARALLEL(Default,K-MEANS++알고리즘이용)
    // val initalizationMode = KMeans.K_MEANS_PARALLEL
    // 센터에 수렴하는 거리의임계값(모든센터가 유클리드거리(1e-4 : default)보다 적은경우 하나의 반복을 정지)
    // val numEpsilon = 1e-4

    // numRuns는 디폴트값이 1인데 Spark 2.0.0 에서 Deprecated.

    
    // 반복 알고리즘으로 데이터를 캐싱하여 속도를 높인다.
    VectorData.cache


    // 학습하는 두가지 방법
    // 1. train(data,k,iteration)
    val KMeansModel = KMeans.train(VectorData, numClusters, MaxIterations)

    // 2. setMethod(), run(data) 
    // Kmeans.setK(numClusters)
    // Kmeans.setMaxIterations(MaxIterations)
    // Kmenas.setInitalizationMode(initailzationMode)
    // Kemans.setEpsilon(numEpsilon)
    
    
    // K-Means Cost : 가장 가까운 센터와의 제곱 거리의 합
    val KMeansCost = KMeansModel.computeCost(VectorData)

    println("Input Data rows : " + VectorData.count())
    println("K Means Cost : " + KMeansCost)

    // 각 클러스터의 센터의 백터 정보
    KMeansModel.clusterCenters.foreach{println}

    
    // 클러스터의 멤버 예측 
    val clusterRddInt = KMeansModel.predict( VectorData )

    // 각 클러스터의 멤버 수 (ClusterIdx,Count) Map List
    val clusterCount = clusterRddInt.countByValue

    // Map List 출력
    clusterCount.toList.foreach{println}
   
}
