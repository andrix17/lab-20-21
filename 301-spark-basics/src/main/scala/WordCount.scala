import org.apache.spark.sql.SparkSession

// spark2-submit --class WordCount BD-301-spark-basics.jar
object WordCount extends App {

  override def main(args: Array[String]): Unit = {

    val username = "agiannini"

    val spark = SparkSession.builder.appName("WordCount Spark 2.1").getOrCreate()

    // Setup default parameters
    val dataset = if (args.length >= 1) args(0) else "divinacommedia"

    // Create an RDD from the files in the given folder
    val rddDC = spark.sparkContext.textFile("hdfs:/bigdata/dataset/" + dataset)

    val rddResult = rddDC
      .flatMap(x => x.split(" "))
      .map(x => (x,1))
      .reduceByKey((x,y) => x + y)

    rddResult.saveAsTextFile("hdfs:/user/" + username + "/spark/WordCount")
  }

}