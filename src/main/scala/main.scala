import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig

trait ConnectionHelper {

  def getSparkContext(args: Array[String]): SparkContext = {
    getSparkSession(args).sparkContext
  }

  def getSparkSession(args: Array[String]): SparkSession = {
    val uri = scala.util.Properties.envOrElse("MONGODB_URI", "mongodb://localhost/event-log.logs" )

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

object Program extends ConnectionHelper {
  /**
   * Run this main method to see the output of this quick example or copy the code into the spark shell
   *
   * @param args takes an optional single argument for the connection string
   * @throws Throwable if an operation fails
   */
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(args) // Don't copy and paste as its already configured in the shell
    val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)
    val s3BucketName = System.getenv("S3_BUCKET_NAME")

    import com.mongodb.spark._
    import com.mongodb.spark.config._
    import org.bson.Document

    // get today & yesterday
    import java.util.Calendar
    import java.text.SimpleDateFormat
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val input: String = args.headOption.getOrElse("")
    var date = ""
    var bound = ""
    if (input.isEmpty) {
      val cal = Calendar.getInstance()
      val today = dateFormat.format(cal.getTime())
      cal.add(Calendar.DATE, -1)
      val yesterday = dateFormat.format(cal.getTime())
      date = yesterday
      bound = today
    } else {
      date = input
      val cal = Calendar.getInstance()
      cal.setTime(dateFormat.parse(input))
      cal.add(Calendar.DATE, 1)
      bound = dateFormat.format(cal.getTime())
    }

    val aggregation = s"""
      {"$$addFields": { date: { "$$dateFromString": { dateString: "$$timestamp"  }  } }}
      {"$$match": { date: {$$gte: ISODate("$date"), $$lt: ISODate("$bound")} } }
      {"$$project" : { date: 0, "@timestamp": 0 } }
    """
    println("aggregation: ")
    println(aggregation)

    // Loading and analyzing data from MongoDB
    val rdd = MongoSpark.load(sc)
    val aggregatedRdd = rdd.withPipeline(Seq(Document.parse(aggregation)))
    println("ROWS COUNT: " + aggregatedRdd.count)
    val df = aggregatedRdd.toDF()
    df.write.parquet(s"s3a://$s3BucketName/$date")
    sc.stop
  }

}
