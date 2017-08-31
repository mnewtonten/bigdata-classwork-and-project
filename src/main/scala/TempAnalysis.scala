object TempAnalysis{
  def main(args:Array[String]){

case class TempData(day: Int, doy: Int, month: Int, year: Int, precip: Double, tave: Double, tmax: Double, tmin: Double)

val source = io.Source.fromFile("/users/mlewis/CSCI1320-S17/SanAntonioTemps.csv")
val lines = source.getLines.drop(2)
val tempData = lines.map { line =>
	val p = line.split(",")
	TempData(p(0).toInt, p(1).toInt, p(2).toInt,
		p(4).toInt, p(5).toDouble, p(6).toDouble,
		p(7).toDouble, p(8).toDouble)
}.toArray

/*
foldLeft(0 -> 0)((agg,td)=>()
{ 
  case((sum,count),td)=>
    

}

*/
println(tempData.reduceLeft((a, b) => if(a.tmax > b.tmax) a else b))
println(tempData.foldLeft(tempData(0))((a, b) => if(a.tmax > b.tmax) a else b))
println(tempData.maxBy(_.tmax))
println(tempData.exists(_.precip > 12))
/*
println(tempData.foldLeft(tempData(0)-> 0){
  
})
*/


val rainTemps = tempData.flatMap(td => if(td.precip >= 1) Seq(td.tmax) else Seq.empty)
rainTemps.take(5) foreach println

val (tempSum, tempCnt) = tempData.foldLeft(0.0 -> 0.0) {case ((sum,cnt),td) => 
  if(td.precip >= 1) (sum+td.tmax, cnt+1) else (sum,cnt)
}
println(tempSum/tempCnt)

val highMonth:Map[Int, Array[TempData]] = tempData.groupBy(_.month)
val monthlyAveTemp = highMonth.map {case (month, tds) =>
  val aveTemp = tds.foldLeft(0.0) { case (sum, td) => sum+td.tmax } / tds.length
  (month, aveTemp)
}

monthlyAveTemp.toSeq.sortBy(_._1) foreach println

println

val monthlyAvePrecip = highMonth.map {case (month,tds) =>
  val avePrecip = tds.foldLeft(0.0) {case (sum, td) => sum+td.precip} /tds.length
  (month, avePrecip)
}

val monthlyMedPrecip = highMonth.map {case (month,tds) =>
  val sortedPrecip = tds.sortBy(_.precip)
  (month, sortedPrecip(tds.length/2))
}

monthlyAvePrecip.toSeq.sortBy(_._1) foreach println

println

monthlyMedPrecip.toSeq.sortBy(_._1) foreach println
source.close

}
}
