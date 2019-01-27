case class checkeOne(id:String,regex:String)
case class checkeTwo(id:String,regex:String,where:String)

case class checkResult(id:String,result:Boolean)

trait Rule[A] {
  def check(r : A) : checkResult
}


object Rule {

  val checkWithJustRegex : Rule[checkeOne] = new Rule[checkeOne] {
     def check(r:checkeOne) : checkResult = {
          checkResult("1",true)
     }
  }
}


