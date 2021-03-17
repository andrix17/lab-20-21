import org.apache.spark.sql.SparkSession

// spark2-submit --class InvertedIndex BD-301-spark-basics.jar
object InvertedIndex extends App {

  override def main(args: Array[String]): Unit = {

    val username = "agiannini"

    val spark = SparkSession.builder.appName("InvertedIndex Spark 2.1").getOrCreate()

    val dataset = if (args.length >= 1) args(0) else "divinacommedia"

    val rddDC = spark.sparkContext.textFile("hdfs:/bigdata/dataset/" + dataset)

    val rddResult = rddDC
      .flatMap(x => x.split(" "))
      .zipWithIndex
      .groupByKey

    rddResult.saveAsTextFile("hdfs:/user/" + username + "/spark/InvertedIndex")
  }

}