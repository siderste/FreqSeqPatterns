package patterns

import database.PostgreSQL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.fpm.PrefixSpan
import patterns.MyPrefixSpan.{Postfix, Prefix}

import scala.collection.mutable

object MyPrefixSpanExample {

  def main(args: Array[String]): Unit = {
    //to debug start it like the following and remove provided from build.sbt, also mind paths
    //val spark = SparkSession.builder.master("local").appName("FreqSeqPatterns").getOrCreate()
    //else for clusters the following
    val spark = SparkSession.builder.appName("FreqSeqPatterns").getOrCreate()

    import spark.implicits._

    //val mydata = spark.sparkContext.textFile("data/mydata/sequences.txt")
    val mydata = spark.sparkContext.textFile("hdfs://localhost:9000/freqseqpatterns/datasets/sequences3.txt")
      .map { sequence =>
        val itemsets = sequence.split(";")
        itemsets.map { itemset => {
          val itemandtime = itemset.split(",")
          Array( new Item(itemandtime(0).toInt, itemandtime(1).toInt) ) //each itemset contains one item
          }
        }
      }.zipWithIndex().map{tupl=>(tupl._2, tupl._1)}

    val postfix = new Postfix(Array(
      new Item(0,-1),new Item(1,0),
      new Item(0,-1),new Item(1,120),
      new Item(0,-1),new Item(2,140),
      new Item(0,-1),new Item(3,150),
      new Item(0,-1),new Item(1,180),
      new Item(0,-1),new Item(2,200),
      new Item(0,-1),new Item(3,220),
      //new Item(0,-1),new Item(2,130),
      //new Item(0,-1),new Item(3,150),
      //new Item(0,-1),new Item(1,180),
      new Item(0,-1)),
      60, 0, Array(), -1, 0)

    val prefix = new Prefix(Array(0,1,0,2),2, mutable.Map.empty)
/*
    val postfixes = postfix.projectFull(prefix)
    println("postfixes="+postfixes.mkString("[",",","]"))
    val postfixes2 = postfix.project(prefix)
    println("postfixes2="+postfixes2.toString)
    val postfixes3 = postfix.projectFull2(prefix)
    println("postfixes3="+postfixes3.mkString("[",",","]"))

    throw new NotImplementedError("operation not yet implemented")
    System.in.read()
*/
    val sparkDataFramePrefixSpan = new PrefixSpan()

    val result = new MyPrefixSpan()
      .setMinSupport(0.4)
      .setMaxPatternLength(50)
      .setMaxLocalProjDBSize(32000000)
      .setMaxDTofPatterns(80)
      .run(mydata.cache())

    result.freqSequences.collect().foreach { freqSequence => // filter(p=>p.sequence.length>1).
      print( freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]")+": " + freqSequence.freq)
      print(", instances=>[")
      freqSequence.instances.foreach{ key=>{
        print(key._1+"->"+key._2.mkString("[",",","]")+",")
      }}
      print("]")
      println()
    }
    // $example off$

    //result.printSchema()
    /*
    root
    |-- sequence: array (nullable = false)
    |    |-- element: array (containsNull = true)
    |    |    |-- element: string (containsNull = true)
    |-- freq: long (nullable = false)
    */
/*
    val concatArray = udf((value:  Seq[Seq[String]]) => {
      value.map(_.mkString("[", ",", "]"))
    })

    val dfr = result.withColumn("sequence", concatArray($"sequence"))
    //dfr.printSchema()
    //dfr.show( dfr.count().toInt, false)

    val postgres = new PostgreSQL(spark)
    postgres.setUpConnection() //comment if not used
    postgres.writeOverwriteToDB( dfr ,
      "localhost","5433","infolab","ais_data","patterns_raw_spark")
*/
    throw new NotImplementedError("operation not yet implemented")
  }
}
