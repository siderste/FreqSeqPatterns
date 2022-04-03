/**
  * Created by stylianos on 12/19/17.
  */

package patterns

import org.apache.spark.internal.Logging

case class Item (d_1: Long = -1, area_id: Int = 0) extends Logging with Serializable with Ordered[Item]{

  override def toString: String = {
    "("+area_id+", "+d_1+")"
  }
  object Item {

  }

  override def compare(that: Item): Int = this.d_1 compare that.d_1
}


