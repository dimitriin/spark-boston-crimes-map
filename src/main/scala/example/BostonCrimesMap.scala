package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, collect_list, concat_ws, row_number, split, trim}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.expressions.Window

object BostonCrimesMap extends App{
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  if (args.length < 3) {
    println("Missed input arguments")

    System.exit(1)
  }

  val crimeCsv = args(0)
  val codeCsv = args(1)
  val outputParquet = args(2)

  val crimeDF = session
    .read
    .option("header","true")
    .csv(crimeCsv)
    .dropDuplicates("INCIDENT_NUMBER")
    .withColumn("OFFENSE_CODE", col("OFFENSE_CODE").cast(IntegerType))
    .withColumn("Lat", col("Lat").cast(DoubleType))
    .withColumn("Long", col("Long").cast(DoubleType))

  crimeDF.printSchema()

  crimeDF.createOrReplaceTempView("crime")

  val offenseCodeDF = session
    .read
    .option("header","true")
    .csv(codeCsv)
    .dropDuplicates("CODE")
    .withColumn("CODE", col("CODE").cast(IntegerType))
    .withColumn("crime_type", trim(split(col("NAME"), "-").getItem(0)))
    .drop("NAME")

  offenseCodeDF.printSchema()

  val districtFrequentCrimeTypesDf = session.sql("SELECT DISTRICT, OFFENSE_CODE FROM crime")
    .join(broadcast(offenseCodeDF), $"OFFENSE_CODE" === $"CODE")
    .groupBy("DISTRICT", "crime_type")
    .count()
    .withColumn("rn", row_number.over(Window.partitionBy($"DISTRICT").orderBy($"count".desc, $"crime_type".asc)))
    .where($"rn" <= 3)
    .groupBy("DISTRICT")
    .agg(concat_ws(", ", collect_list("crime_type")).as("frequent_crime_types"))

  session
    .sql("SELECT DISTRICT, YEAR, MONTH FROM crime")
    .groupBy('DISTRICT, 'YEAR, 'MONTH).count()
    .createOrReplaceTempView("crimes_count_by_district_year_month")

  val districtCrimesTotalAndMonthlyDf = session
    .sql("SELECT DISTRICT, sum(count) as crimes_total, percentile_approx(count, 0.5) as crimes_monthly FROM crimes_count_by_district_year_month GROUP BY DISTRICT")

  val districtAvgLat = session
    .sql("SELECT DISTRICT, Lat FROM crime")
    .groupBy("DISTRICT")
    .avg("Lat")
    .alias("Lat")
    .withColumnRenamed("avg(Lat)", "lat")

  val districtAvgLong = session
    .sql("SELECT DISTRICT, Long FROM crime")
    .groupBy("DISTRICT")
    .avg("Long")
    .withColumnRenamed("avg(Long)", "lng")

  val resultDf = districtCrimesTotalAndMonthlyDf
    .join(districtFrequentCrimeTypesDf, Seq("DISTRICT"))
    .join(districtAvgLat, Seq("DISTRICT"))
    .join(districtAvgLong, Seq("DISTRICT"))
    .withColumnRenamed("DISTRICT", "district")

  resultDf
    .repartition(1)
    .write
    .format("parquet")
    .mode("append")
    .save(outputParquet)
}
