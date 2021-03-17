import org.apache.spark.sql.SparkSession

// spark2-submit --class WordLengthCount BD-301-spark-basics.jar
object WordLengthCount extends App {

  override def main(args: Array[String]): Unit = {

    val username = "agiannini"

    val spark = SparkSession.builder.appName("WordLengthCount Spark 2.1").getOrCreate()

    val dataset = if (args.length >= 1) args(0) else "divinacommedia"

    val rddDC = spark.sparkContext.textFile("hdfs:/bigdata/dataset/" + dataset)

    val rddResult = rddDC
      .flatMap(x => x.split(" "))
      .map(x => (x.length,1))
      .reduceByKey((x,y) => x + y)

    rddResult.saveAsTextFile("hdfs:/user/" + username + "/spark/WordLengthCount")
  }

}