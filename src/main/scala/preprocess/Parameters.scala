package preprocess

import java.net.URL
import java.util.Properties

import scala.io.Source

class Parameters(fileURL: URL) extends Serializable {

  val properties = new Properties()
  properties.load(Source.fromURL(fileURL).bufferedReader())

  def getPropertyValue(property:String):String={
    properties.getProperty(property)
  }

  def setPropertyValue(property:String, value:String)={
    properties.setProperty(property, value)
  }

  val inputData = getPropertyValue("storage") + "/" + getPropertyValue("data_file")

  properties.setProperty("inputData", inputData)

  override def toString: String = {
    properties.toString
  }

}
