/**
  * Created by stylianos on 12/19/17.
  */

package patterns

import org.apache.spark.internal.Logging

case class Item (val id: Int, val timestamp: Long) extends Logging with Serializable with Ordered[Item]{

  override def toString: String = {
    "("+id+", "+timestamp+")"
  }
  object Item {

  }

  override def compare(that: Item): Int = this.timestamp compare that.timestamp
}
