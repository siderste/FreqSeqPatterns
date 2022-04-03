package patterns

import org.apache.spark.internal.Logging

case class TrajOfItems (d_0: Long, items: Array[Item]) extends Logging with Serializable{

  override def toString: String = {
    "("+d_0+", "+items.toString+")"
  }

}
