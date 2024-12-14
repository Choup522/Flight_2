import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import LoggerFactory.logger
import scala.collection.mutable
import JoinPhases._
import Restatements._
import Library._

// Creation of the logger object
object LoggerFactory {
  val logger: Logger = Logger.getLogger("Main_Logger")
}

// Main functions of the program
object main {

  def main(args: Array[String]): Unit = {

    // Initialize the logger
    logger.info("Main: Starting the program")

    // Retrieve the logger object
    val config: Config = ConfigFactory.load("config")
    if (config.isEmpty) {
      logger.error("Main: Configuration file 'config.json' not found or is empty in the classpath.")
    }
    require(!config.isEmpty, "Configuration file 'config.json' not found or is empty in the classpath.")

    // Get the environment
    val paths = config.getConfig("paths")
    val sizeOfSample = config.getDouble("execution.sizeOfSample")

    // Creation of spark session
    logger.info("Main: Creating Spark session")
    val spark = SparkSession.builder()
      .appName("Flight")
      .master("local[*]")
      .getOrCreate()

    // Calculate the optimal number of partitions to use
    val partitions = calculateOptimalPartitions(spark.sparkContext)
    logger.info(s"Main: Optimal number of partitions: $partitions")

    // Creation of the execution times array
    val executionTimes = mutable.ArrayBuffer[(String, Double)]()

    // get the path from the configuration file
    val datapath_flight = paths.getString("datapath_flight")
    val datapath_weather = paths.getString("datapath_weather")
    val datapath_wban = paths.getString("datapath_wban")
    val output = paths.getString("output_dataframe")

    // Load the data
    logger.info("Main: Loading the data")
    val startLoadData = System.nanoTime()
    val flights = spark.read.option("header", "true").csv(datapath_flight)
    val weather = spark.read.option("header", "true").csv(datapath_weather)
    val wban = spark.read.option("header", "true").csv(datapath_wban)
    val endLoadData = System.nanoTime()
    val durationLoadData = (endLoadData - startLoadData) / 1e9d
    executionTimes += (("Load data", durationLoadData))

    // Create sample data
    val startCreationOfSample = System.nanoTime()
    val flights_sample = flights.sample(sizeOfSample, seed=42).persist()
    val weather_sample = weather.sample(sizeOfSample, seed=42).persist()
    val endCreationOfSample = System.nanoTime()
    val durationCreationOfSample = (endCreationOfSample - startCreationOfSample) / 1e9d
    executionTimes += (("Create samples", durationCreationOfSample))

    val countFlight = flights_sample.count()
    val countWeather = weather_sample.count()

    logger.info(s"Main: Number of rows in the flights sample: $countFlight")
    logger.info(s"Main: Number of rows in the weather sample: $countWeather")

    // Prepare the data
    logger.info("Main: Preparing the data")
    val startRestatements = System.nanoTime()
    val FT_Table = createFlightTable(flights_sample, wban)
    val OT_Table = createObservationTable(weather_sample, wban)
    val endRestatements = System.nanoTime()
    val durationRestatements = (endRestatements - startRestatements) / 1e9d
    executionTimes += (("Restatements", durationRestatements))

    // Join the data
    logger.info("Main: Joining the data - first step")
    val startJoinOperations = System.nanoTime()
    val FT_Table_prepared = DF_Map(FT_Table, "FT")
    val OT_Table_prepared = DF_Map(OT_Table, "OT")

    logger.info("Main: Joining the data - second step")
    val finalDF_Cols = DF_Reduce_Cols(FT_Table_prepared, OT_Table_prepared, partitions)
    exportSchema(finalDF_Cols, output + "FinalDF_schema.json")

    val endJoinOperations = System.nanoTime()
    val durationJoinOperations = (endJoinOperations - startJoinOperations) / 1e9d
    executionTimes += (("JoinOperations", durationJoinOperations))

    // Creating the filtered dataframe
    val startFiltering = System.nanoTime()
    val onTimeThreshold: Double = 15
    val missingValueThreshold: Double = 0.8
    val finalDF = generateFlightDatasets2(finalDF_Cols, onTimeThreshold, missingValueThreshold)

    exportSchema(finalDF, output + "finalDF_schema.json")
    finalDF.persist()

    val count_finalDF = finalDF.count()
    logger.info(s"Main: Number of rows in the  dataset: $count_finalDF")

    val endFiltering = System.nanoTime()
    val durationFiltering = (endFiltering - startFiltering) / 1e9d
    executionTimes += (("Filtering", durationFiltering))

    // Random Forest
    logger.info("Main: Random Forest")
    val startRandomForrest = System.nanoTime()

    // Definition of label and features columns
    val labelCol = "FT_OnTime"
    val featureCols = finalDF.columns.filter(_ != labelCol)
    val trees: Int = 10
    val depth: Int = 5
    val k : Int = 4

    //df_for_ml.cache()
    val metrics = RandomForest.randomForest(finalDF, labelCol, featureCols, trees, depth, k, partitions)
    saveMetricsAsCSV(spark, metrics, output + "metrics.csv")

    val endRandomForrest = System.nanoTime()
    val durationRandomForrest = (endRandomForrest - startRandomForrest) / 1e9d
    executionTimes += (("RandomForrest", durationRandomForrest))

    // Creation of dataframe from execution times
    val executionTimesDF = createExecutionTimesDataFrame(spark, executionTimes.toIndexedSeq)
    exportDataToCSV(executionTimesDF, output + "execution_times.csv")

    // Un-persist the data
    finalDF.unpersist()

    logger.info("Main: Stopping the program")
  }
}

