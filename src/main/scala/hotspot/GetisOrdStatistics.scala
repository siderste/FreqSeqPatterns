package hotspot

class GetisOrdStatistics(inSum: Double, inSquareSum: Double, inNumOfItems: Long)
  extends Serializable {

  var sum = inSum
  var squareSum = inSquareSum
  var numOfItems = inNumOfItems
  var mean = sum / numOfItems.toDouble
  var std = Math.sqrt( (squareSum / numOfItems.toDouble) - Math.pow(mean, 2) )

  override def toString: String = {
    "GetisOrdStatistics [sum="+sum+", squareSum="+squareSum+", mean=" + mean + ", std=" + std + ", numOfItems=" + numOfItems + "]"
  }

}
