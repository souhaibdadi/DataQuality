package fr.edf.dco.edma.configuration.checkes

class RegexWhere(id:String,regex:String) extends Check {

  val typ:String ="RegexWhere"
  var wheres:List[Where] = List()

  def getId = this.id
  def getRegex = this.regex

  def addWhere(where: Where): Unit = {
    this.wheres = where :: wheres
  }

  def getListOfWhere() = this.wheres

}
