import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

object hive_simple extends App
{
    val sparkCxt = new SparkContext()

    val hiveCxt = new HiveContext(sparkCxt)


    // RDD 데이터 형 변환을 위한 패키지
    import hiveCxt.implicits._

    // Add UDFRowSeqeunce Package
    hiveCxt.sql("add jar /opt/cloudera/parcels/CDH/jars/hive-contrib-0.13.1-cdh5.3.8.jar")

    // Create temporary function 
    hiveCxt.sql("create temporary function row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence'")


    // Query
    val data = hiveCxt.sql("select row_sequence(),* from default.test limit 5")

    // RDD Count
    println( data.count())

    // RDD to Array
    data.collect().foreach(println)

    // RDD to Custom data Array
    data.map( t=> t(0) + " " + t(1)+ " " +t(2) ).collect().foreach(println)


}
