# Flight Data Analysis

## Description
This project is designed to analyze flight data using Apache Spark. It includes data loading, preprocessing, and running a Random Forest algorithm to predict flight delays.

## Prerequisites
- Scala 3.3.4
- SBT 1.5.5 or later
- Apache Spark 3.5.1
- Java 8 or later

## Project Structure
- `build.sbt`: SBT build file with project dependencies.
- `src/main/scala/main.scala`: Main application code.
- `spark-run.sh`: Script to run the Spark application.
- `.gitignore`: Git ignore file to exclude unnecessary files from version control.

## Setup
1. Clone the repository:
    ```sh
    git clone <https://github.com/Choup522/Flight_2>
    cd <https://github.com/Choup522/Flight_2>
    ```

2. Build the project:
    ```sh
    sbt clean compile
    ```

3. Run the tests:
    ```sh
    sbt test
    ```

## Running the Application
1. Package the application:
    ```sh
    sbt package
    ```

2. Run the Spark application:
    ```sh
    ./spark-run.sh
    ```

## Configuration
The application uses a configuration file named `config.json` located in the classpath. Ensure this file is correctly set up with the necessary paths and parameters.

## Logging
The application uses a logger to log important information and errors. Check the logs for detailed execution information.

## Results of the analysis
The application will output the results of the analysis to the console. 
The results include the accuracy of the Random Forest model :

| Metric      | Based on columns | Based on lines |
|-------------|------------------|----------------|
| Accuracy    | 79,69%           | 80,83%         |
| F1 Score    | 70,68%           | 72,27%         |
| Recall      | 79,69%           | 80,84%         |
| Precision   | 63,50%           | 65,35%         |

Executions of the Random Forest model with different parameters are also available in the logs: 
The executions are expressed in seconds.

| Task             | Based on columns | Based on lines |
|------------------|------------------|----------------|
| Load data        | 10,99            | 11,96          |
| Create samples   | 0,13             | 0,26           |
| Restatements     | 1,56             | 1,68           |
| JoinOperations   | 7,64             | 1,81           |
| Filtering        | 444,41           | 118,09         |
| RandomForrest    | 109,23           | 701,26         |


## License
This project is licensed under the MIT License.