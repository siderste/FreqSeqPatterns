/**
  * Created by stylianos on 12/19/17.
  */

package patterns

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.clustering.meanshift.{DenseDoubleVector, DoubleVector}

case class Item (time: Long = -1, area_id: Int = 0, spatial: Array[Double] = Array(0.0,0.0,0.0) )
  extends Logging with Serializable with Ordered[Item]{

  override def toString: String = {
    "("+area_id+", "+time+", "+spatial+")"
  }
  object Item {

  }

  def toSpatialDoubleVector: DoubleVector = {
    new DenseDoubleVector(this.spatial)
  }

  def toSpatialDenseVector: Vector = {
    new DenseVector(this.spatial)
  }

  override def compare(that: Item): Int = this.time compare that.time
}


