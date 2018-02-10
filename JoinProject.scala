import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, DateTimeZone}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object JoinProject {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("Join App").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val currentDir = System.getProperty("user.dir")  // get the current directory
    val inputFile = "file://" + currentDir + "/input_file.txt"
    val outputDir = "file://" + currentDir + "/output/"

    joinUsingRDDs(sc, inputFile, outputDir + "rdd/")
    joinUsingSQL(spark, inputFile, outputDir + "sql/")
    joinUsingDFs(spark, inputFile, outputDir + "df/")
    joinUsingDSs(sc, spark, inputFile, outputDir + "ds/")
    joinManually(sc, inputFile, outputDir + "manual/")
    joinOptimized(sc, inputFile, outputDir + "optimized/")
  }

  def joinUsingRDDs(sc: SparkContext, inputFile: String, outputDir: String): Unit = {
    val inputRDD = sc.textFile(inputFile, 2)
    val s_relation = inputRDD.filter(line => line.charAt(0) == 'S')
      .map(line => line.split(","))
      .map(line => (line(1), line(2)))
    val r_relation = inputRDD.filter(line => line.charAt(0) == 'R')
      .map(line => line.split(","))
      .map(line => (line(1), line(2)))

    val startTime = DateTime.now(DateTimeZone.UTC).getMillis()
    val joined = r_relation.join(s_relation)
    val numOfresults = joined.count()
    val totalTime = (DateTime.now(DateTimeZone.UTC).getMillis()-startTime)
    println("[RDD join] \t Time: " + totalTime + " miliseconds \t Number of results: " + numOfresults)
    joined.saveAsTextFile(outputDir)
  }

  def joinUsingDFs(spark: SparkSession, inputFile: String, outputDir: String): Unit = {
    val df = spark.sqlContext.read.option("header", "false").option("delimiter", ",").csv(inputFile)
    val cols = Seq[String]("_c1", "_c2")
    val s_df = df.filter("_c0 == 'S'").select(cols.head, cols.tail: _*).toDF(Seq("key", "valueS"): _*)
    val r_df = df.filter("_c0 == 'R'").select(cols.head, cols.tail: _*).toDF(Seq("key", "valueR"): _*)
    val startTime = DateTime.now(DateTimeZone.UTC).getMillis()
    val joined = r_df.join(s_df, Seq("key"))
    val numOfresults = joined.count()
    val totalTime = (DateTime.now(DateTimeZone.UTC).getMillis()-startTime)
    println("[DataFrame join] \t Time: " + totalTime + " miliseconds \t Number of results: " + numOfresults)
    joined.write.csv(outputDir)
  }

  def joinUsingSQL(spark: SparkSession, inputFile: String, outputDir: String): Unit = {
    val df = spark.sqlContext.read.option("header", "false").option("delimiter", ",").csv(inputFile)
    val cols = Seq[String]("_c1", "_c2")
    df.filter("_c0 == 'S'").select(cols.head, cols.tail: _*).toDF(Seq("key", "valueS"): _*).createOrReplaceTempView("s_table")
    df.filter("_c0 == 'R'").select(cols.head, cols.tail: _*).toDF(Seq("key", "valueR"): _*).createOrReplaceTempView("r_table")
    val startTime = DateTime.now(DateTimeZone.UTC).getMillis()
    val joined = spark.sql("SELECT s_table.key, s_table.valueS, r_table.valueR FROM s_table INNER JOIN r_table ON s_table.key = r_table.key")
    val numOfresults = joined.count()
    val totalTime = (DateTime.now(DateTimeZone.UTC).getMillis()-startTime)
    println("[DataFrame + SQL join] \t Time: " + totalTime + " miliseconds \t Number of results: " + numOfresults)
    joined.write.csv(outputDir)
  }

  def joinUsingDSs(sc: SparkContext, spark: SparkSession, inputFile: String, outputDir: String): Unit = {
    import spark.implicits._
    val myData = spark.read.option("header", "false").option("delimiter", ",").textFile(inputFile)
    val s_relationship = myData.filter(line => line.charAt(0) == 'S').map(line => line.split(",")).map(line => (line(1), line(2)))
    val r_relationship = myData.filter(line => line.charAt(0) == 'R').map(line => line.split(",")).map(line => (line(1), line(2)))
    val startTime = DateTime.now(DateTimeZone.UTC).getMillis()
    val joined = s_relationship.joinWith(r_relationship, s_relationship("_1") === r_relationship("_1"))

    val numOfresults = joined.count()
    val totalTime = (DateTime.now(DateTimeZone.UTC).getMillis()-startTime)
    println("[DataSet join] \t Time: " + totalTime + " miliseconds \t Number of results: " + numOfresults)
    joined.write.json(outputDir)
  }

  def joinManually(sc: SparkContext, inputFile: String, outputDir: String): Unit = {
    val myData = sc.textFile(inputFile,2)
    val startTime = DateTime.now(DateTimeZone.UTC).getMillis
    val joined = myData.map(line => line.split(",")).map(arrLine => (arrLine(1), (arrLine(0), arrLine(1), arrLine(2)))).groupByKey()
      .flatMapValues(tuples => tuples.filter(tuple => tuple._1 == "R").flatMap(tuple1 => tuples.filter(tuple => tuple._1 == "S").map(tuple2 => (tuple1._3,tuple2._3))))
    val numOfresults = joined.count()
    val totalTime = (DateTime.now(DateTimeZone.UTC).getMillis()-startTime)
    println("[Manual join] \t Time: " + totalTime + " miliseconds \t Number of results: " + numOfresults)
    joined.saveAsTextFile(outputDir)
  }

  def joinOptimized(sc: SparkContext, inputFile: String, outputDir: String): Unit = {
    val myData = sc.textFile(inputFile,2)
    val startTime = DateTime.now(DateTimeZone.UTC).getMillis()
    val joined = myData.map(line => line.split(",")).map(arrLine => ((arrLine(1), arrLine(0)), (arrLine(0), arrLine(2))))
      .repartitionAndSortWithinPartitions(new CustomPartitioner(2)).map(arrLine => (arrLine._1._1, arrLine._2)).groupByKey()
      .flatMap(tuple => {
        var listOfR : List[String] = List()
        tuple._2.map(record => {
          if (record._1 == "R") {
            listOfR = listOfR :+ record._2
          } else
            listOfR.map(valueR => (tuple._1, (valueR, record._2)))
        })
      })
      .filter(_.isInstanceOf[List[(String, (String, String))]]).map(_.asInstanceOf[List[(String, (String, String))]]).flatMap(x => x)

    val numOfresults = joined.count()
    val totalTime = (DateTime.now(DateTimeZone.UTC).getMillis()-startTime)
    println("[Optimized manual join] \t Time: " + totalTime + " miliseconds \t Number of results: " + numOfresults)
    joined.saveAsTextFile(outputDir)
  }
}

class CustomPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    key match {
      case tuple @ (a: String, b: String) => Integer.valueOf(a) % 2
    }
  }
}