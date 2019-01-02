package fr.edf.dco.edma.edq.configuration.checkes

import fr.edf.dco.edma.configuration.checkes.Check

@SerialVersionUID(16L)
class FieldsChecks(field:String) extends Serializable {

  var checks:List[Check] = List()

  def addCheck(check: Check): Unit = {
    this.checks = check :: checks
  }

  def getField() = this.field

}

