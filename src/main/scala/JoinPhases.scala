import LoggerFactory.logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import Library._

case object JoinPhases {

  // Function to prepare the DataFrame for the join operation
  def DF_Map(in_DF: DataFrame, in_DF_Type: String): DataFrame = {

    logger.info(s"DF_Map: Processing DataFrame of type $in_DF_Type")

    // Adding a column to identify the type of DataFrame
    var out_Map = in_DF //.withColumn("TAG", lit(in_DF_Type))
    //logger.info(s"DF_Map: Added TAG column with value $in_DF_Type")

    // Operation for the weather observation table (OT)
    if (in_DF_Type == "OT") {

      logger.info(s"DF_Map: Processing OT DataFrame")

      // Creation of the join key JOIN_KEY by concatenating ORIGIN_AIRPORT_ID and Date
      out_Map = out_Map.withColumn("OT_JOIN_KEY", concat(col("OT_ORIGIN_AIRPORT_ID"), lit("_"), col("OT_Date")))
      logger.info(s"DF_Map: Created OT_JOIN_KEY column")

    } else if (in_DF_Type == "FT") {

      logger.info(s"DF_Map: Processing FT DataFrame")

      // Creation of the column DEST_DATE_TIME representing the time of arrival at the destination airport and conversion in seconds
      out_Map = out_Map.withColumn("FT_DEST_DATE_TIME", from_unixtime(unix_timestamp(col("FT_TIMESTAMP")) + (col("FT_CRS_ELAPSED_TIME") * 60).cast("long") + (col("FT_Delta_Lag") * 3600).cast("long")).cast(TimestampType))
      logger.info(s"DF_Map: Created FT_DEST_DATE_TIME column")

      // Creation of the column ORIGIN_DATE_TIME representing the time of departure from the origin airport and conversion in seconds
      //out_Map = out_Map.withColumn("FT_DEST_DATE_TIME", (col("FT_TIMESTAMP") + col("FT_CRS_ELAPSED_TIME") * 60 + col("FT_Delta_Lag") * 3600).cast(TimestampType))

      // Creation of the join key for the origin airport JOIN_KEY_ORIGIN
      out_Map = out_Map.withColumn("FT_JOIN_KEY_ORIGIN", concat(col("FT_ORIGIN_AIRPORT_ID"), lit("_"), col("FT_FL_DATE")))
      logger.info(s"DF_Map: Created FT_JOIN_KEY_ORIGIN column")

      // Creation of the join key for the destination airport JOIN_KEY_DEST
      out_Map = out_Map.withColumn("FT_JOIN_KEY_DEST", concat(col("FT_DEST_AIRPORT_ID"), lit("_"), col("FT_DEST_DATE_TIME")))
      logger.info(s"DF_Map: Created FT_JOIN_KEY_DEST column")

    } else {
      logger.error(s"DF_Map: error $in_DF_Type dataframe type not allowed (OT,FT)!")
      throw new IllegalArgumentException(s"DF_Map: error $in_DF_Type dataframe type not allowed (OT,FT) !")
    }

    logger.info(s"DF_Map: Completed processing for DataFrame of type $in_DF_Type")
    out_Map
  }

  // Function to join dataframes based on columns
  def DF_Reduce_Cols(FT_flights: DataFrame, OT_weather: DataFrame, numPartitions: Int = 100) : DataFrame = {

    logger.info("DF_Reduce Cols: Starting DataFrame reduction")

    // Sort datetime in ascending order
    logger.info("DF_Reduce Cols: Sorted weather and flight data by OT_WEATHER_TIMESTAMP")
    val FT_flights_sorted = FT_flights.sort("FT_TIMESTAMP").repartitionByRange(numPartitions, col("FT_TIMESTAMP"))
    val OT_weather_sorted = OT_weather.sort("OT_WEATHER_TIMESTAMP").repartitionByRange(numPartitions, col("OT_WEATHER_TIMESTAMP"))

    // Will loop on hour to generate and retrieve corresponding weather observations
    val hours_lag = 0 to 11

    // Generate columns for lag hours
    logger.info("DF_Reduce Cols: Creating columns for lag hours")
    val df_flights = FT_flights_sorted.select(
      col("*") +: hours_lag.flatMap(h => Seq(
        (col("FT_TIMESTAMP") - expr(s"INTERVAL $h HOURS")).as(s"FT_ORIGIN_DATE_TIME_PART_$h"),
        (col("FT_DEST_DATE_TIME") - expr(s"INTERVAL $h HOURS")).as(s"FT_DEST_DATE_TIME_PART_$h")
      )): _*
    )

    // Join the flights with the weather data
    logger.info("DF_Reduce Cols: Joined the dataframes")

    var df_result = df_flights

    for (h <- hours_lag) {
      logger.info(s"DF_Reduce Cols: Performing join for lag hour $h")

      // Create temporary weather dataframe for origin join
      val df_weather_origin = OT_weather_sorted
        .withColumn(s"OT_DATE_TIME_Part_$h", col("OT_WEATHER_TIMESTAMP"))
        .select(col(s"OT_DATE_TIME_Part_$h") +: OT_weather_sorted.columns.map(c => col(c).alias(s"${c}_Part_$h")): _*)

      // Create temporary weather dataframe for destination join
      val df_weather_dest = OT_weather_sorted
        .withColumn(s"OT_DEST_DATE_TIME_Part_$h", col("OT_WEATHER_TIMESTAMP"))
        .select(col(s"OT_DEST_DATE_TIME_Part_$h") +: OT_weather_sorted.columns.map(c => col(c).alias(s"${c}_Dest_Part_$h")): _*)
      logger.info(s"DF_Reduce Cols: Prepared weather data for destination join for lag hour $h")

      // Join flights with weather data on origin date
      df_result = df_result
        .join(df_weather_origin, col(s"FT_ORIGIN_DATE_TIME_PART_$h") === col(s"OT_DATE_TIME_Part_$h"), "left")
        .join(df_weather_dest, col(s"FT_DEST_DATE_TIME_PART_$h") === col(s"OT_DEST_DATE_TIME_Part_$h"), "left")
      logger.info(s"DF_Reduce Cols: Completed join for lag hour $h")
    }

    // Remove columns
    logger.info("DF_Reduce Cols: Removed columns")
    val joinColumns_ft_origin = (0 until 12).map(i => s"FT_ORIGIN_DATE_TIME_PART_$i")
    val joinColumns_ot_origin = (0 until 12).map(i => s"OT_DATE_TIME_Part_$i")
    val joinColumns_ft_destination = (0 until 12).map(i => s"FT_DEST_DATE_TIME_PART_$i")
    val joinColumns_ot_destination = (0 until 12).map(i => s"OT_DEST_DATE_TIME_Part_$i")
    val joinColumns_ot_wban = (0 until 12).map(i => s"OT_WBAN_Part_$i")
    val joinColumns_ot_Join_key = (0 until 12).map(i => s"OT_JOIN_KEY_Part_$i")
    val joinColumns_ot_Date = (0 until 12).map(i => s"OT_Date_Part_$i")
    val joinColumns_ot_Time = (0 until 12).map(i => s"OT_Time_Part_$i")

    // Fusion of all columns
    val allJoinColumns = joinColumns_ft_origin ++ joinColumns_ot_origin ++ joinColumns_ft_destination ++ joinColumns_ot_destination ++ joinColumns_ot_wban ++ joinColumns_ot_Join_key ++ joinColumns_ot_Date ++ joinColumns_ot_Time
    df_result = df_result.drop(allJoinColumns: _*)

    df_result.drop("FT_JOIN_KEY_ORIGIN", "FT_JOIN_KEY_DEST")

    // Repartition the joined dataframe
    logger.info("DF_Reduce Cols: Repartitioned the joined dataframe")
    val joined_DF_repartitioned = df_result.repartition(numPartitions)

    joined_DF_repartitioned
  }

  // Function to join dataframes based on lines
  def DF_Reduce_Lines(FT_flights: DataFrame, OT_weather: DataFrame, numPartitions: Int = 100, spark: SparkSession) : DataFrame = {

    logger.info("DF_Reduce Line: Starting DataFrame reduction")
    import spark.implicits._

    // Step 1: Create a dataset of hours_lag from 0 to 11
    logger.info("DF_Reduce Line: Created a dataset of hours_lag from 0 to 11")
    val hours_lag = spark.createDataset(0 to 11).toDF("hours_lag")

    val df_flights_with_lag = FT_flights
      .withColumn("FT_ORIGIN_UNIX_TS", unix_timestamp($"FT_TIMESTAMP"))
      .withColumn("FT_DEST_UNIX_TS", unix_timestamp($"FT_DEST_DATE_TIME"))
      .crossJoin(hours_lag)
      .withColumn("FT_ORIGIN_DATE_TIME_LAG", expr("from_unixtime(FT_ORIGIN_UNIX_TS - hours_lag * 3600)"))
      .withColumn("FT_DEST_DATE_TIME_LAG", expr("from_unixtime(FT_DEST_UNIX_TS - hours_lag * 3600)"))
      .repartition($"FT_ORIGIN_AIRPORT_ID", $"FT_DEST_AIRPORT_ID")

    logger.info("DF_Reduce_Line: Created lagged timestamps for each flight")
    val df_weather_origin = addSuffixToColumns(OT_weather, "_ORIG")
    val df_weather_dest = addSuffixToColumns(OT_weather, "_DEST")

    logger.info("DF_Reduce_Line: Repartitioned the dataframes")
    val df_weather_origin_partitioned = df_weather_origin.repartition($"OT_ORIGIN_AIRPORT_ID_ORIG")
    val df_weather_dest_partitioned = df_weather_dest.repartition($"OT_ORIGIN_AIRPORT_ID_DEST")

    logger.info("DF_Reduce_Line: Joined the dataframes")
    val df_result = df_flights_with_lag
      .join(broadcast(df_weather_origin_partitioned), col("FT_ORIGIN_DATE_TIME_LAG") === col("OT_WEATHER_TIMESTAMP_ORIG") && col("FT_ORIGIN_AIRPORT_ID") === col("OT_ORIGIN_AIRPORT_ID_ORIG"), "left")
      .join(broadcast(df_weather_dest_partitioned), col("FT_DEST_DATE_TIME_LAG") === col("OT_WEATHER_TIMESTAMP_DEST") && col("FT_DEST_AIRPORT_ID") === col("OT_ORIGIN_AIRPORT_ID_DEST"), "left")

    df_result
  }

  def chooseJoinOpération(parameter: String, FT_table: DataFrame, OT_table: DataFrame, numPartitions: Int = 100, output: String, spark: SparkSession): DataFrame = {

    val finalDF = parameter match {
      case "cols" =>
        val finalDF_Cols = DF_Reduce_Cols(FT_table, OT_table, numPartitions)
        exportSchema(finalDF_Cols, output + "FinalDF_schema.json")
        finalDF_Cols
      case "lines" =>
        val finalDF_Lines = DF_Reduce_Lines(FT_table, OT_table, numPartitions, spark)
        exportSchema(finalDF_Lines, output + "FinalDF_schema.json")
        finalDF_Lines
      case _ =>
        throw new IllegalArgumentException(s"Invalid parameter value: $parameter. Accepted values are 'cols' or 'lines'.")
    }

    finalDF

  }

}
