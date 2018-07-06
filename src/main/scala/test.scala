import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig

/**
 * A helper for the tour
 */
trait TourHelper {

  def getSparkContext(args: Array[String]): SparkContext = {
    getSparkSession(args).sparkContext
  }

  def getSparkSession(args: Array[String]): SparkSession = {
    val uri: String = args.headOption.getOrElse("mongodb://localhost/event-log.logs")

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MongoSparkConnectorTour")
      .set("spark.app.id", "MongoSparkConnectorTour")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)
      .set("spark.mongodb.input.database", "event-log")
      .set("spark.mongodb.input.collection", "logs")

    val session = SparkSession.builder().config(conf).getOrCreate()
    MongoConnector(session.sparkContext)//.withDatabaseDo(WriteConfig(session), {db => db.drop()})
    session
  }

}

object Introduction extends TourHelper {

  /**
   * Run this main method to see the output of this quick example or copy the code into the spark shell
   *
   * @param args takes an optional single argument for the connection string
   * @throws Throwable if an operation fails
   */
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(args) // Don't copy and paste as its already configured in the shell

    import com.mongodb.spark._
    import com.mongodb.spark.config._

    // Loading and analyzing data from MongoDB
    val rdd = MongoSpark.load(sc)
    println("---------------------------")
    println("COUNT:" + rdd.count)
    println(rdd.first.toJson)
    println("---------------------------")
    sc.stop
  }

}
