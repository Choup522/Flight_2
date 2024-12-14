import LoggerFactory.logger
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import Library._
import org.apache.spark.sql.functions._

case object Restatements {

  // Function to create FT Table
  def createFlightTable(df: DataFrame, wban_df: DataFrame): DataFrame = {

    logger.info("Restatements: Creating Flight Table")

    // Remove the column "Unnamed: 12" and format the columns
    val cleaned_df = df
      .drop("_c12")
      .withColumn("CRS_DEP_TIME", lpad(col("CRS_DEP_TIME").cast(IntegerType).cast(StringType), 4, "0")) // Fill with 0 on the left to manage 1 or 2-digit times
      .withColumn("CRS_DEP_TIME", expr("substring(CRS_DEP_TIME, 1, 2) || ':' || substring(CRS_DEP_TIME, 3, 2)"))
      .withColumn("FL_DATE", to_date(col("FL_DATE"), "yyyy-MM-dd"))
      .withColumn("ARR_DELAY_NEW", col("ARR_DELAY_NEW").cast(DoubleType))
      .withColumn("CANCELLED", col("CANCELLED").cast(IntegerType))
      .withColumn("DIVERTED", col("DIVERTED").cast(IntegerType))
      .withColumn("CRS_ELAPSED_TIME", col("CRS_ELAPSED_TIME").cast(DoubleType))
      .withColumn("WEATHER_DELAY", col("WEATHER_DELAY").cast(DoubleType))
      .withColumn("NAS_DELAY", col("NAS_DELAY").cast(DoubleType))
      .withColumn("TIMESTAMP", to_timestamp(concat(col("FL_DATE"), lit(" "), col("CRS_DEP_TIME")), "yyyy-MM-dd HH:mm"))
      .na.fill(0, Seq("ARR_DELAY_NEW", "WEATHER_DELAY", "NAS_DELAY"))

    // Calculation of jet lag for origin and destination airports
    var newdf = cleaned_df
      .join(broadcast(wban_df), cleaned_df("ORIGIN_AIRPORT_ID") === wban_df("AirportID"), "inner")
      .select(wban_df("TimeZone"), cleaned_df("*"))
      .withColumnRenamed("TimeZone", "ORIGIN_TIME_ZONE")

    newdf = newdf
      .join(broadcast(wban_df), cleaned_df("DEST_AIRPORT_ID") === wban_df("AirportID"), "inner")
      .withColumnRenamed("TimeZone", "DEST_TIME_ZONE")
      .withColumn("Delta_Lag", col("DEST_TIME_ZONE").cast(IntegerType) - col("ORIGIN_TIME_ZONE").cast(IntegerType))

    // Remove diverted and cancelled flights
    newdf = newdf
      .where(col("DIVERTED") =!= 1 && col("CANCELLED") =!= 1)
      .drop("DIVERTED", "CANCELLED")
      .drop("ORIGIN_TIME_ZONE", "DEST_TIME_ZONE")
      .drop("OP_CARRIER_AIRLINE_ID", "OP_CARRIER_FL_NUM")

    newdf = addPrefixToColumns(newdf, "FT_")

    newdf

  }

  // Function to create OT Table
  def createObservationTable(df: DataFrame, wban_df: DataFrame): DataFrame = {

    logger.info("Restatements: Creating Weather Table")

    // Conversion columns to Double format
    val columnsToConvert = List("StationType","Visibility","DryBulbFarenheit","DryBulbCelsius","WetBulbFarenheit","WetBulbCelsius","DewPointFarenheit","DewPointCelsius","RelativeHumidity", "WindSpeed","WindDirection","ValueForWindCharacter","StationPressure","PressureTendency","PressureChange" ,"SeaLevelPressure", "HourlyPrecip", "Altimeter")
    val cleaned_df = Library.convertColumnsToDouble(df, columnsToConvert)

    // List of flag columns
    val flagColumns = Seq("SkyConditionFlag", "VisibilityFlag", "WeatherTypeFlag", "DryBulbFarenheitFlag", "DryBulbCelsiusFlag", "WetBulbFarenheitFlag", "WetBulbCelsiusFlag", "DewPointFarenheitFlag", "DewPointCelsiusFlag", "RelativeHumidityFlag", "WindSpeedFlag", "WindDirectionFlag", "ValueForWindCharacterFlag", "StationPressureFlag", "PressureTendencyFlag", "PressureChangeFlag", "SeaLevelPressureFlag", "RecordTypeFlag", "HourlyPrecipFlag", "AltimeterFlag")

    // Remove flag columns and join with WBAN DataFrame
    var newdf = cleaned_df
      .join(broadcast(wban_df), Seq("WBAN"), "inner")
      .withColumnRenamed("AirportID", "ORIGIN_AIRPORT_ID")
      .drop(flagColumns: _*)
      .withColumn("Time", lpad(col("Time"), 4, "0"))
      .withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
      .withColumn("WEATHER_TIMESTAMP", to_timestamp(concat(col("DATE"), lit(" "), col("TIME")), "yyyy-MM-dd HHmm"))

    newdf = addPrefixToColumns(newdf, "OT_")

    newdf

  }

  // Function to generate flight datasets with correlation analysis
  def generateFlightDatasets2(df: DataFrame, onTimeThreshold: Double, missingValueThreshold: Double): DataFrame = {

    logger.info("Restatements: Generating Flight Datasets with Correlation Analysis")

    // Add the FT_OnTime column
    logger.info("Restatements: Adding FT_OnTime column")
    val newdf = df.withColumn("FT_OnTime", when(col("FT_ARR_DELAY_NEW") <= onTimeThreshold, 1).otherwise(0))

    // Clean dataframe from missing values in columns
    logger.info("Restatements: Cleaning missing values")
    val reducedDf = cleanMissingValues(newdf, missingValueThreshold)
    val finalDf = imputeMissingValues(reducedDf, 1 - missingValueThreshold)

    finalDf
  }

  // Function to impute missing values in a DataFrame
  private def imputeMissingValues(df: DataFrame, missingThreshold: Double = 0.20): DataFrame = {

    // Select numeric columns
    val numericColumns = df.schema.fields.filter(field => field.dataType == IntegerType || field.dataType == DoubleType).map(_.name)

    // Calculate total number of rows
    val totalRows = df.count()

    // Filter columns to impute based on missing values threshold
    val columnsToImpute = numericColumns.filter { colName =>
      val missingCount = df.where(df(colName).isNull || df(colName) === "" || df(colName).isNaN).count()
      val missingPercentage = missingCount.toDouble / totalRows
      missingPercentage < missingThreshold
    }

    // if there are columns to impute
    if (columnsToImpute.nonEmpty) {

      // Initialize the Imputer
      val imputer = new Imputer()
        .setInputCols(columnsToImpute)
        .setOutputCols(columnsToImpute)
        .setStrategy("mean") // ou "median"

      // Apply the imputer
      val model = imputer.fit(df)
      val imputedDF = model.transform(df)

      // Return the imputed DataFrame
      imputedDF

    } else {

      // Return the original DataFrame without modifications
      df

    }
  }
}
