import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig

trait ConnectionHelper {

  def getSparkContext(uri: String): SparkContext = {
    getSparkSession(uri).sparkContext
  }

  def getSparkSession(uri: String): SparkSession = {
    val conf = new SparkConf()
      // .setMaster("local[*]")
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
    val input: String = args.headOption.getOrElse("")
    println("input date: ")
    println(input)
    val uri: String = args(1) //System.getenv("MONGODB_URI")
    println("uri: ")
    println(uri)
    val s3BucketName: String = args(2)
    println("S3_BUCKET_NAME: ")
    println(s3BucketName)

    val sc = getSparkContext(uri)
    // val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
    // val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    // sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
    // sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)

    import com.mongodb.spark._
    import com.mongodb.spark.config._
    import org.bson.Document

    // get today & yesterday
    import java.util.Calendar
    import java.text.SimpleDateFormat
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

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

    val addFields = s"""{"$$addFields": { date: { "$$dateFromString": { dateString: "$$timestamp"  }  } }}"""
    val matchDate = s"""{"$$match": { date: {$$gte: ISODate("$date"), $$lt: ISODate("$bound")} } }"""
    val project = s"""{"$$project" : { date: 0, "@timestamp": 0 } }"""
    println("aggregation: ")
    println(addFields)
    println(matchDate)
    println(project)

    // Loading and analyzing data from MongoDB
    val rdd = MongoSpark.load(sc)
    val aggregatedRdd = rdd.withPipeline(Seq(
      Document.parse(addFields),
      Document.parse(matchDate),
      Document.parse(project)
    ))
    val count = aggregatedRdd.count
    println("ROWS COUNT: " + count)
    if (count == 0) {
      println(s"WARNING: $date COUNT = 0")
    } else {
      val Array(year, month, day) = date.split("-")
      val destination = s"s3n://$s3BucketName/year=$year/month=$month/day=$day"
      println("Data will be written to: " + destination)
      val df = aggregatedRdd.toDF()
      df.write.parquet(destination)
    }
    sc.stop
  }

}
