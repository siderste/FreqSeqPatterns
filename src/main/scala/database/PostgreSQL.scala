package database

import org.apache.spark.mllib.clustering.meanshift.PointWithTrajid
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{explode, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.util.List
import java.util.Properties
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

class PostgreSQL(spark: SparkSession) extends Serializable {

  val connectionProperties = new Properties()

  def setUpConnection() = {
    connectionProperties.put("user", "postgres")
    //connectionProperties.put("password", "WR. 967nd@t@A")
    connectionProperties.put("password", "postgres")
    connectionProperties.put("driver", "org.postgresql.Driver")
  }

  def writeAppendToDB(clusteringFlag: DataFrame, dbHost: String, dbPort: String, dbDatabase: String, dbSchema: String, dbTable: String) = {

    clusteringFlag.write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", "jdbc:postgresql://"+dbHost+":"+dbPort+"/"+dbDatabase)
      //.option("url", "jdbc:postgresql://localhost:5432/datasets")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", dbSchema+"."+dbTable)
      .option("user", "postgres")
      //.option("password", "WR. 967nd@t@A")
      .option("password", "postgres")
      //.option("createTableColumnTypes", "from_id INTEGER, to_id INTEGER, weight INTEGER")
      .save()
  }

  def writeOverwriteRDD2ToDB(patternsAndClusters2: RDD[((String, Int), mutable.Map[Integer, List[PointWithTrajid]])]): DataFrame = {
    import spark.implicits._
    val freqPatternsSchema = StructType(Seq(
        StructField("pattern", StringType, true),
        StructField("pattern_length", IntegerType, true),
        StructField("cluster_id", IntegerType, true),
        StructField("trajectory", LongType, true),
        StructField("vector", ArrayType(DoubleType), false)
    ))
    var all_rows = patternsAndClusters2.flatMap{case( (p,pl),m )=>
      m.flatMap{case( k, v )=>
        v.asScala.toList.map(point => {
          Row(p, pl, k, point.getId, point.getVec.toArray)
        })
      }
    }
    var df = spark.createDataFrame(all_rows, freqPatternsSchema)
    df.printSchema()
    df
  }

  def writeOverwriteRDDToDB(patternTransitionsRDD: RDD[((String, Int), mutable.Map[Long, Array[linalg.Vector]])]): DataFrame = {
    import spark.implicits._
    val freqPatternsSchema = new StructType()
      .add("pattern", StringType, true)
      .add("pattern_length", IntegerType, true)
      .add("transitions", MapType(LongType, ArrayType(new VectorUDT())), true)
    var all_rows = patternTransitionsRDD.map(row=>{
      Row( row._1._1, row._1._2, row._2 )
    })
    val vecToSeq = udf((v: linalg.Vector) => v.toArray).asNondeterministic
    var df = spark.createDataFrame(all_rows, freqPatternsSchema)
      .select($"pattern",$"pattern_length", explode($"transitions"))
      .select($"pattern",$"pattern_length",$"key".as("trajectory"),explode($"value").as("points"))
      .withColumn("points", vecToSeq($"points"))
    df.printSchema()
    df
  }

  def writeOverwriteToDB(nodes: DataFrame, dbHost: String, dbPort: String, dbDatabase: String, dbSchema: String, dbTable: String) = {
    nodes.write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      //.option("url", "jdbc:postgresql://localhost:45436/siderDatacron")
      .option("url", "jdbc:postgresql://"+dbHost+":"+dbPort+"/"+dbDatabase)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", dbSchema+"."+dbTable)
      .option("user", "postgres")
      .option("password", "WR. 967nd@t@A")
      //.option("password", "postgres")
      //.option("createTableColumnTypes", "from_id INTEGER, to_id INTEGER, weight INTEGER")
      .save()
  }

  def readDBdataToDataFrame (dbSchema: String, dbHost: String, dbPort: String, dbDatabase: String, dbTable: String): DataFrame = {
    /*
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:45436/siderDatacron")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "public.meanshift_clusters")
      .option("user", "postgres")
      .option("password", "WR. 967nd@t@A")
      .load()
      */
    val jdbcDF = spark.read
      //.jdbc("jdbc:postgresql://localhost:45436/siderDatacron", "public."+dbTable, connectionProperties)
      .jdbc("jdbc:postgresql://"+dbHost+":"+dbPort+"/"+dbDatabase, dbSchema+"."+dbTable, connectionProperties)
    jdbcDF
  }
}
