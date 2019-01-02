package fr.edf.dco.edma.edq.dataNode

case class DataNode(rowKey: String) {
  var data: Map[String, String] = Map()
  def add(key: String, value: String): DataNode = {
    data = data + (key -> value)
    this
  }

  def getField(field:String) : Option[String] = {
      data.get(field)
  }

  def isExist(field:String) : Boolean = {
    data.isDefinedAt(field)
  }

}
