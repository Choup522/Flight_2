import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.io.PrintWriter

object Library {

  // Function for adding prefixes to DataFrame columns
  def addPrefixToColumns(df: DataFrame, prefix: String): DataFrame = {
    df.columns.foldLeft(df)((tempDF, colName) => tempDF.withColumnRenamed(colName, s"$prefix$colName"))
  }

  // Function to rename all columns with a suffix
  def addSuffixToColumns(df: DataFrame, suffix: String): DataFrame = {
    df.columns.foldLeft(df) { (newDF, colName) =>
      newDF.withColumnRenamed(colName, colName + suffix)
    }
  }

  // Function to convert columns to Double
  def convertColumnsToDouble(df: DataFrame, columns: List[String]): DataFrame = {
    columns.foldLeft(df) { (tempDf, colName) => tempDf.withColumn(colName, col(colName).cast("Double")) }
  }

  // Function to save the metrics to a file
  def saveMetricsAsCSV(spark: SparkSession, metrics: Map[String, Double], outputPath: String): Unit = {

    import spark.implicits._

    // Conversion of the metrics to a DataFrame
    val metricsDF = metrics.toSeq.toDF("metric", "value")

    // Save in CSV format
    metricsDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputPath/metrics.csv")

  }

  // Function to export the schema of a DataFrame
  def exportSchema(df: DataFrame, outputPath: String): Unit = {

    // Retrieve the schema in JSON format
    val schemaJson = df.schema.json

    // Write the schema to a file
    new PrintWriter(outputPath) {
      write(schemaJson)
      close()
    }
  }

  // Function to calculate the optimal partitioning
  def calculateOptimalPartitions(sparkContext: SparkContext, multiplier: Int = 2): Int = {
    val cores = sparkContext.getConf.get("spark.executor.cores", "1").toInt
    val executors = sparkContext.getConf.get("spark.executor.instances", "1").toInt
    val totalCores = cores * executors

    // Calculate of partitions
    val partitions = totalCores * multiplier
    partitions
  }

  // Function to create a DataFrame from a sequence of tuples
  def createExecutionTimesDataFrame(spark: SparkSession, times: Seq[(String, Double)]): DataFrame = {
    import spark.implicits._
    times.toDF("Task", "Duration (seconds)")
  }

  // Function for exporting data in csv format
  def exportDataToCSV(df: DataFrame, outputPath: String): Unit = {
    df
      .coalesce(1) // Save as a single file
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(outputPath)
  }

  // Function to count missing values
  def cleanMissingValues(df: DataFrame, threshold: Double): DataFrame = {

    // Number of lines in the DataFrame
    val totalRows = df.count()

    // Calculate the number of missing values for each column
    val missingCounts = df.columns.map { colName =>
      count(when(col(colName).isNull || col(colName) === "", 1)).alias(colName)
    }

    // Create a DataFrame containing the missing values for each column
    val missingCountsDf = df.select(missingCounts: _*)

    // Calculate the percentage of missing values for each column
    val missingPercentages = missingCountsDf.head().getValuesMap[Any](df.columns).map {
      case (colName, missingCount) =>
        val percentage = missingCount.toString.toDouble / totalRows
        (colName, percentage)
    }

    // Keep only the columns that respect the threshold
    val columnsToKeep = missingPercentages.collect {
      case (colName, percentage) if percentage <= threshold => colName
    }.toSeq

    // Filter the DataFrame to keep only the valid columns
    df.select(columnsToKeep.map(col): _*)
  }

}
