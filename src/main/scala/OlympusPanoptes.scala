import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.locationtech.spatial4j.distance.DistanceCalculator
import org.locationtech.spatial4j.context.SpatialContext
import org.locationtech.spatial4j.shape.Point
import org.locationtech.spatial4j.distance.GeodesicSphereDistCalc
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

object OlympusPanoptes extends App {

  // Initialize Spark Session
  val spark = SparkSession.builder
    .appName("OlympusPanoptes")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Might need to swap over to datasets API for performance, at which point this will go into a utils file as a case class
  // Define schema for JSON data
  val schema = new StructType()
    .add("coalition", StringType)
    .add("type", StringType)
    .add("callsign", StringType)
    .add("northing", StringType)
    .add("easting", StringType)
    .add("altitude", IntegerType)
    .add("heading", IntegerType)

  // Helper function to convert DMS to decimal degrees
  def dmsToDd(dms: String): Double = {
    val parts = dms.split("'")
    val degrees = parts(0).toDouble
    val minutes = parts(1).toDouble
    val seconds = parts(2).toDouble
    degrees + (minutes / 60) + (seconds / 3600)
  }

  // UDF to convert DMS to decimal degrees
  val dmsToDdUDF = udf(dmsToDd _)

  // Function to calculate distance between two points
  // Function to calculate distance between two points
  def calculateDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val ctx = SpatialContext.GEO
    val point1: Point = ctx.getShapeFactory.pointXY(lon1, lat1)
    val point2: Point = ctx.getShapeFactory.pointXY(lon2, lat2)
    val distanceCalculator: DistanceCalculator = new GeodesicSphereDistCalc.Haversine()
    distanceCalculator.distance(point1, point2) * 1000 // Convert to meters
  }

  // UDF to calculate distance
  val calculateDistanceUDF = udf(calculateDistance _)

  // Function to calculate bearing between two points
  def calculateBearing(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val lat1Rad = Math.toRadians(lat1)
    val lon1Rad = Math.toRadians(lon1)
    val lat2Rad = Math.toRadians(lat2)
    val lon2Rad = Math.toRadians(lon2)
    val dLon = lon2Rad - lon1Rad
    val x = Math.sin(dLon) * Math.cos(lat2Rad)
    val y = Math.cos(lat1Rad) * Math.sin(lat2Rad) - (Math.sin(lat1Rad) * Math.cos(lat2Rad) * Math.cos(dLon))
    val initialBearing = Math.toDegrees(Math.atan2(x, y))
    (initialBearing + 360) % 360
  }

  // UDF to calculate bearing
  val calculateBearingUDF = udf(calculateBearing _)

  // Function to categorize aspect
  def categorizeAspect(numericalAspect: Double): String = {
    numericalAspect match {
      case x if x <= 30 || x >= 330 => "hot"
      case x if x >= 150 && x <= 210 => "cold"
      case x if x > 30 && x <= 90 || x >= 270 && x < 330 => "beaming"
      case x if x > 90 && x < 150 || x > 210 && x < 270 => "flanking"
      case _ => "unknown"
    }
  }

  // UDF to categorize aspect
  val categorizeAspectUDF = udf(categorizeAspect _)

  // Function to categorize altitude differential
  def categoriseAltitudeDifferential(fromAltitude: Int, toAltitude: Int): String = {
    val differential = fromAltitude - toAltitude
    differential match {
      case diff if diff > 1000 => "low"
      case diff if diff < -1000 => "high"
      case _ => "co altitude"
    }
  }

  // UDF to categorize altitude differential
  val categorizeAltitudeDifferentialUDF = udf(categoriseAltitudeDifferential _)

  // Function to clean callsign
  def cleanCallsign(callsign: String): String = {
    callsign.replaceAll("\\W+", "").toLowerCase
  }

  // UDF to clean callsign
  val cleanCallsignUDF = udf(cleanCallsign _)

  // Read JSON data as streaming source
  val inputDF = spark.readStream
    .schema(schema)
    .json("path to some json file with all the aircraft data")

  // Convert northing and easting to decimal degrees
  val latLongFormattedDF = inputDF
    .withColumn("northing_dd", dmsToDdUDF($"northing"))
    .withColumn("easting_dd", dmsToDdUDF($"easting"))

  // Filter blue and red coalitions
  val bluforDF = latLongFormattedDF.filter($"coalition" === "blue")
  val redforDF = latLongFormattedDF.filter($"coalition" === "red")

  // Create Cartesian product of blue and red DataFrames
  val redBlueCrossJoinedDF = bluforDF.crossJoin(redforDF)
    .withColumn("distance", calculateDistanceUDF($"northing_dd", $"easting_dd", $"northing_dd", $"easting_dd"))
    .withColumn("bearing", calculateBearingUDF($"northing_dd", $"easting_dd", $"northing_dd", $"easting_dd"))
    .withColumn("altitude_differential", categorizeAltitudeDifferentialUDF($"altitude", $"altitude"))
    .withColumn("numerical_aspect", abs($"bearing" - $"heading") % 360)
    .withColumn("aspect", categorizeAspectUDF($"numerical_aspect"))
    .withColumn("from_clean", cleanCallsignUDF($"callsign"))

  // Select necessary columns
  val columnsToKeep = Seq(
    "coalition", "type", "callsign", "altitude", "heading", "distance", "bearing", "altitude_differential", "numerical_aspect", "aspect", "from_clean"
  )
  val lookupDF = redBlueCrossJoinedDF.select(columnsToKeep.head, columnsToKeep.tail: _*)

  // Write the output to a location
  val query = lookupDF.writeStream
    .outputMode("append")
    .format("json")
    .option("path", "path/to/output")
    .option("checkpointLocation", "path/to/checkpoint")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  query.awaitTermination()

}