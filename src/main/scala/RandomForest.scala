import LoggerFactory.logger
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{PCA, StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.types.{DateType, StringType, TimestampType}
import org.apache.spark.storage.StorageLevel

case object RandomForest {

  def randomForest(df: DataFrame, labelCol: String, featureCols: Array[String], trees: Int, depth: Int, k: Int, NumPartitions: Int): Map[String, Double] = {

    try {
      logger.info("Random Forest : Starting the process")

      // Check if columns exist in the dataframe
      //logger.info(""Random Forest : Validating feature and label columns")
      //val missingColumns = (featureCols :+ labelCol).filterNot(df.columns.contains)
      //if (missingColumns.nonEmpty) {
      //  val errorMsg = s"Missing columns in DataFrame: ${missingColumns.mkString(", ")}"
      //  logger.error(errorMsg)
      //  throw new IllegalArgumentException(errorMsg)
      //}

      // Identify string and date columns in the feature columns
      val stringColumns = df.schema.fields.filter(_.dataType == StringType).map(_.name).intersect(featureCols)
      val dateColumns = df.schema.fields.filter(f => f.dataType == DateType || f.dataType == TimestampType).map(_.name).intersect(featureCols)

      // Conversion of date in numerical values
      logger.info("\"Random Forest : Converting date and timestamp columns to numeric values")
      var dfTransformed = df
      for (col <- dateColumns) {
        dfTransformed = dfTransformed.withColumn(col, unix_timestamp(dfTransformed(col)).cast("double"))
      }

      // Repartition the DataFrame for better parallelism
      logger.info(" \"Random Forest :Repartitioning the DataFrame for better parallelism")
      dfTransformed = dfTransformed.repartition(NumPartitions)

      // Cleaning existing indexed columns
      //logger.info("Random Forest : Checking for and removing existing indexed columns")
      //val indexedColNames = stringColumns.map(c => s"${c}_indexed")
      //dfTransformed = indexedColNames.foldLeft(dfTransformed) { (tempDF, colName) =>
      //  if (tempDF.columns.contains(colName)) tempDF.drop(colName) else tempDF
      //}

      // Creation of indexed columns for string columns
      logger.info("Random Forest : Creating StringIndexers for string columns")
      val indexers: Array[PipelineStage] = stringColumns.map { colName =>
        new StringIndexer()
          .setInputCol(colName)
          .setOutputCol(s"${colName}_indexed")
          .setHandleInvalid("skip")
      }

      // Assemble the feature columns
      val indexedFeatureCols = featureCols.map { col =>
        if (stringColumns.contains(col)) s"${col}_indexed" else col
      }

      val assembler = new VectorAssembler()
        .setInputCols(indexedFeatureCols)
        .setOutputCol("features")

      // Add PCA for dimensionality reduction
      logger.info(s"Adding PCA with $k components")
      val pca = new PCA()
        .setInputCol("features")
        .setOutputCol("pcaFeatures")
        .setK(k)

      // Index the label column
      val labelIndexer = new StringIndexer()
        .setInputCol(labelCol)
        .setOutputCol("indexedLabel")
        .setHandleInvalid("skip")

      // Configure the Random Forest model
      val randomForest = new RandomForestClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("pcaFeatures")
        .setNumTrees(trees)
        .setMaxDepth(depth)

      // Create the pipeline
      logger.info("Creating the pipeline")
      val pipeline = new Pipeline().setStages(indexers :+ labelIndexer :+ assembler :+pca :+ randomForest)

      // Divide the data into training and test sets
      logger.info("Splitting data into training and test sets")
      val Array(trainingData, testData) = dfTransformed.randomSplit(Array(0.8, 0.2), seed = 42L)
      trainingData.persist(StorageLevel.MEMORY_AND_DISK)
      testData.persist(StorageLevel.MEMORY_AND_DISK)

      val countTrainingData = trainingData.count()
      val countTestData = testData.count()
      val countColumnsTrainingData = trainingData.schema.fields.length
      val countColumnsTestData = testData.schema.fields.length

      logger.info(s"Random Forest : Number of rows in the training dataset: $countTrainingData")
      logger.info(s"Random Forest : Number of rows in the test dataset: $countTestData")
      logger.info(s"Random Forest : Number of columns in the training dataset: $countColumnsTrainingData")
      logger.info(s"Random Forest : Number of columns in the test dataset: $countColumnsTestData")

      // Check that the sets are not empty
      if (trainingData.isEmpty) {
        val errorMsg = "Random Forest : Training dataset is empty after splitting."
        logger.error(errorMsg)
        throw new IllegalArgumentException(errorMsg)
      }
      if (testData.isEmpty) {
        val errorMsg = "Random Forest : Test dataset is empty after splitting."
        logger.error(errorMsg)
        throw new IllegalArgumentException(errorMsg)
      }

      // Train the model
      logger.info("Random Forest : Training the model")
      val model = pipeline.fit(trainingData)

      // Predict on the test data
      logger.info(" Random Forest :Making predictions")
      val predictions = model.transform(testData)

      // Evaluate the model
      logger.info("Random Forest : Evaluating the model")
      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")

      val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
      val f1Score = evaluator.setMetricName("f1").evaluate(predictions)
      val recall = evaluator.setMetricName("weightedRecall").evaluate(predictions)
      val precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)

      // Collect the metrics
      logger.info("Random Forest : Collecting metrics")
      val metrics = Map(
        "accuracy" -> accuracy,
        "f1Score" -> f1Score,
        "recall" -> recall,
        "precision" -> precision
      )

      // Clean up persisted resources
      logger.info(" Random Forest :Cleaning up persisted data")
      trainingData.unpersist()
      testData.unpersist()

      // Print the metrics
      metrics

    } catch {
      case e: IllegalArgumentException =>
        logger.error(s" Random Forest :IllegalArgumentException encountered: ${e.getMessage}", e)
        throw e
      case e: Exception =>
        logger.error(s"Random Forest : Unexpected error encountered: ${e.getMessage}", e)
        throw e
    }
  }
}
