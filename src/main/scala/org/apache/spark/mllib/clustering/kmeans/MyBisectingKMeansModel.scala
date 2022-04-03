package org.apache.spark.mllib.clustering.kmeans

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.clustering.{DistanceMeasure, VectorWithNorm}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.json4s.JsonDSL._
import org.json4s.{DefaultFormats, _}
import org.json4s.jackson.JsonMethods._

class MyBisectingKMeansModel(
        val root: ClusteringTreeNode,
        @Since("2.4.0") val distanceMeasure: String
      ) extends Serializable with Saveable with Logging {

  @Since("1.6.0")
  def this(root: ClusteringTreeNode) = this(root, MyDistanceMeasure.RV)

  private val distanceMeasureInstance: DistanceMeasure =
    MyDistanceMeasure.decodeFromString(distanceMeasure)

  /**
    * Leaf cluster centers.
    */
  @Since("1.6.0")
  def clusterCenters: Array[Vector] = root.leafNodes.map(_.center)

  /**
    * Number of leaf clusters.
    */
  lazy val k: Int = clusterCenters.length

  /**
    * Predicts the index of the cluster that the input point belongs to.
    */
  @Since("1.6.0")
  def predict(point: Vector): Int = {
    root.predict(point, distanceMeasureInstance)
  }

  /**
    * Predicts the indices of the clusters that the input points belong to.
    */
  @Since("1.6.0")
  def predict(points: RDD[Vector]): RDD[Int] = {
    points.map { p => root.predict(p, distanceMeasureInstance) }
  }

  /**
    * Java-friendly version of `predict()`.
    */
  @Since("1.6.0")
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] =
    predict(points.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Integer]]

  /**
    * Computes the squared distance between the input point and the cluster center it belongs to.
    */
  @Since("1.6.0")
  def computeCost(point: Vector): Double = {
    root.computeCost(point, distanceMeasureInstance)
  }

  /**
    * Computes the sum of squared distances between the input points and their corresponding cluster
    * centers.
    */
  @Since("1.6.0")
  def computeCost(data: RDD[Vector]): Double = {
    data.map(root.computeCost(_, distanceMeasureInstance)).sum()
  }

  /**
    * Java-friendly version of `computeCost()`.
    */
  @Since("1.6.0")
  def computeCost(data: JavaRDD[Vector]): Double = this.computeCost(data.rdd)

  @Since("2.0.0")
  override def save(sc: SparkContext, path: String): Unit = {
    MyBisectingKMeansModel.SaveLoadV2_0.save(sc, this, path)
  }

  override protected def formatVersion: String = "2.0"
}

@Since("2.0.0")
object MyBisectingKMeansModel extends Loader[MyBisectingKMeansModel] {

  @Since("2.0.0")
  override def load(sc: SparkContext, path: String): MyBisectingKMeansModel = {
    val (loadedClassName, formatVersion, __) = Loader.loadMetadata(sc, path)
    (loadedClassName, formatVersion) match {
      case (SaveLoadV1_0.thisClassName, SaveLoadV1_0.thisFormatVersion) =>
        val model = SaveLoadV1_0.load(sc, path)
        model
      case (SaveLoadV2_0.thisClassName, SaveLoadV2_0.thisFormatVersion) =>
        val model = SaveLoadV2_0.load(sc, path)
        model
      case _ => throw new Exception(
        s"BisectingKMeansModel.load did not recognize model with (className, format version):" +
          s"($loadedClassName, $formatVersion).  Supported:\n" +
          s"  (${SaveLoadV1_0.thisClassName}, ${SaveLoadV1_0.thisClassName}\n" +
          s"  (${SaveLoadV2_0.thisClassName}, ${SaveLoadV2_0.thisClassName})")
    }
  }

  private case class Data(index: Int, size: Long, center: Vector, norm: Double, cost: Double,
                          height: Double, children: Seq[Int], members: Seq[(String, Vector)])

  private object Data {
    def apply(r: Row): Data = Data(r.getInt(0), r.getLong(1), r.getAs[Vector](2), r.getDouble(3),
      r.getDouble(4), r.getDouble(5), r.getSeq[Int](6), r.getSeq[(String, Vector)](7))
  }

  private def getNodes(node: ClusteringTreeNode): Array[ClusteringTreeNode] = {
    if (node.children.isEmpty) {
      Array(node)
    } else {
      node.children.flatMap(getNodes) ++ Array(node)
    }
  }

  private def buildTree(rootId: Int, nodes: Map[Int, Data]): ClusteringTreeNode = {
    val root = nodes(rootId)
    if (root.children.isEmpty) {
      new ClusteringTreeNode(root.index, root.size, new VectorWithNorm(root.center, root.norm),
        root.cost, root.height, new Array[ClusteringTreeNode](0), root.members.map{ case(id, vec)=>(id,new VectorWithNorm(vec)) }.toArray )
    } else {
      val children = root.children.map(c => buildTree(c, nodes))
      new ClusteringTreeNode(root.index, root.size, new VectorWithNorm(root.center, root.norm),
        root.cost, root.height, children.toArray, root.members.map{ case(id, vec)=>(id, new VectorWithNorm(vec)) }.toArray )
    }
  }

  private[clustering] object SaveLoadV1_0 {
    private[clustering] val thisFormatVersion = "1.0"

    private[clustering]
    val thisClassName = "org.apache.spark.mllib.clustering.BisectingKMeansModel"

    def save(sc: SparkContext, model: MyBisectingKMeansModel, path: String): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion)
          ~ ("rootId" -> model.root.index)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      val data = getNodes(model.root).map(node => Data(node.index, node.size,
        node.centerWithNorm.vector, node.centerWithNorm.norm, node.cost, node.height,
        node.children.map(_.index), node.members.map{ case(id, vec)=>(id, vec.vector) } ) )
      spark.createDataFrame(data).write.parquet(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): MyBisectingKMeansModel = {
      implicit val formats: DefaultFormats = DefaultFormats
      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)
      val rootId = (metadata \ "rootId").extract[Int]
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val rows = spark.read.parquet(Loader.dataPath(path))
      Loader.checkSchema[Data](rows.schema)
      val data = rows.select("index", "size", "center", "norm", "cost", "height", "children", "members")
      val nodes = data.rdd.map(Data.apply).collect().map(d => (d.index, d)).toMap
      val rootNode = buildTree(rootId, nodes)
      new MyBisectingKMeansModel(rootNode, MyDistanceMeasure.RV)
    }
  }

  private[clustering] object SaveLoadV2_0 {
    private[clustering] val thisFormatVersion = "2.0"

    private[clustering]
    val thisClassName = "org.apache.spark.mllib.clustering.BisectingKMeansModel"

    def save(sc: SparkContext, model: MyBisectingKMeansModel, path: String): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion)
          ~ ("rootId" -> model.root.index) ~ ("distanceMeasure" -> model.distanceMeasure)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      val data = getNodes(model.root).map(node => Data(node.index, node.size,
        node.centerWithNorm.vector, node.centerWithNorm.norm, node.cost, node.height,
        node.children.map(_.index), node.members.map{ case(id, vec)=>(id, vec.vector) } ))
      spark.createDataFrame(data).write.parquet(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): MyBisectingKMeansModel = {
      implicit val formats: DefaultFormats = DefaultFormats
      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)
      val rootId = (metadata \ "rootId").extract[Int]
      val distanceMeasure = (metadata \ "distanceMeasure").extract[String]
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val rows = spark.read.parquet(Loader.dataPath(path))
      Loader.checkSchema[Data](rows.schema)
      val data = rows.select("index", "size", "center", "norm", "cost", "height", "children", "members")
      val nodes = data.rdd.map(Data.apply).collect().map(d => (d.index, d)).toMap
      val rootNode = buildTree(rootId, nodes)
      new MyBisectingKMeansModel(rootNode, distanceMeasure)
    }
  }
}