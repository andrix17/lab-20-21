import org.apache.spark.sql.SparkSession

// spark2-submit --class AverageWordLength BD-301-spark-basics.jar
object AverageWordLength extends App {

  override def main(args: Array[String]): Unit = {

    val username = "agiannini"

    val spark = SparkSession.builder.appName("AverageWordLength Spark 2.1").getOrCreate()

    val dataset = if (args.length >= 1) args(0) else "divinacommedia"

    val rddDC = spark.sparkContext.textFile("hdfs:/bigdata/dataset/" + dataset)

    val rddResult = rddDC
      .flatMap(x => x.split(" "))
      .filter(x => x.length>0)
      .map(x => (x.substring(0,1),x.length))
      .aggregateByKey((0.0,0.0))((a,x) => (a._1 + x, a._2 + 1), (x,y) => (x._1 + y._1, x._2 + y._2))
      .map({case (k,v) => (k, v._1/v._2 )})

    rddResult.saveAsTextFile("hdfs:/user/" + username + "/spark/AverageWordLength")
  }

}