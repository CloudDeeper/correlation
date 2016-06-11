import org.apache.spark._
import org.apache.hadoop.fs._
import scala.collection.mutable.ListBuffer
import java.lang.Double
import java.lang.Integer
import java.lang.Math
import scala.util.control.Breaks._
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation

class Con(s: String) extends Serializable {
	var price: Double = 0
	var amount: Double = 0
	var name: String = s
	var id: String = new String
	var year: Int = 0
	var month: Int = 0
	var day: Int = 0
	var tem: Double = 0
	var rain: Double = 0
	var xx: Double = 0
	var yy: Double = 0
	var coef1: Double = 0
	var coef2: Double = 0
	var coef3: Double = 0
	var isAvg: Int = 0
	var pCount: Int = 0
	var wCount: Int = 0

	def setDay(c: Int) : Unit = {
		this.day = c
	}
	def setPCount(c: Int) : Unit = {
		this.pCount = c
	}	
	def setWCount(c: Int) : Unit = {
		this.wCount = c
	}	
	def setIsAvg() : Unit = {
		this.isAvg = 1
		this.wCount = 1
	}	

	def setY(x: Double) : Unit = {
		this.yy = x
	}

	def setX(x: Double) : Unit = {
		this.xx = x
	}

	def setWeather(t: Double, r: Double) : Unit = {
		this.tem = t
		this.rain = r
	}

	def setMonth(y: Int, m: Int) : Unit = {
		this.year = y
		this.month = m
	}

	def getWCount(): Int = {
		this.wCount
	}
	def getPCount(): Int = {
		this.pCount
	}
	def getTem(): Double = {
		this.tem
	}
	def getRain(): Double = {
		this.rain
	}
	def getPrice(): Double = {
		this.price
	}

	def getAmount(): Double = {
		this.amount
	}
	def setPrice(r: Double): Unit = {
		this.price = r
	}

	def setAmount(r: Double): Unit = {
		this.amount = r
	}

	def setId(ss: String): Unit = {
		this.id = ss
	}

	def +++(right: Con): Con = {
		val ret = new Con(this.name)	
/*
		ret.setWeather((this.tem + right.tem) / 2, (this.rain + right.rain) / 2)
		if(this.price == 0)
			ret.setPrice(right.price)
		else if(right.price == 0)
			ret.setPrice(this.price)
		else
			ret.setPrice((this.price + right.price) / 2)
		ret.setAmount(this.amount + right.amount)
*/
		if(this.year == 0 || right.month == 0)
			ret.setMonth(this.year + right.year, this.month + right.month)
		else
			ret.setMonth(this.year, this.month)
		if(this.coef1==0 && right.coef1 == 0)
		ret.setCoef(this.xx*this.yy + right.xx * right.yy, 
								this.xx*this.xx + right.xx*right.xx, 
								this.yy*this.yy + right.yy*right.yy)
		else if(this.coef1!=0 && right.coef1 == 0)
		ret.setCoef(this.coef1 + right.xx * right.yy, 
								this.coef2 + right.xx*right.xx, 
								this.coef3 + right.yy*right.yy)
		else if(this.coef1==0 && right.coef1 != 0)
		ret.setCoef(this.xx*this.yy + right.coef1, 
								this.xx*this.xx + right.coef2, 
								this.yy*this.yy + right.coef3)
		else
		ret.setCoef(this.coef1 + right.coef1, 
								this.coef2 + right.coef2, 
								this.coef3 + right.coef3)

		ret
		
	}
	def ++(right: Con): Con = {
		val ret = new Con(this.name)	
		ret.setPrice((this.price + right.price))
		ret.setPCount(this.pCount + right.pCount)
		ret.setId(this.id)
		ret.setAmount(this.amount + right.amount)
		ret.setMonth(this.year, this.month)
		ret
	}

	def +(right: Con): Con = {
		val ret = new Con(this.name)	
		ret.setIsAvg()
		ret.setWCount(this.wCount + right.wCount)
		ret.setPCount(this.pCount + right.pCount)
/*
		if(this.isAvg == 0)
			ret.setWeather(right.tem,  right.rain)
		else if(right.isAvg==0)
			ret.setWeather(this.tem,  this.rain)
		else
*/
			ret.setWeather((this.tem+right.tem),  (this.rain+right.rain))
/*
		if(this.price == 0)
			ret.setPrice(right.price)
		else if(right.price == 0)
			ret.setPrice(this.price)
		else
*/
			ret.setPrice((this.price + right.price))
/*
		if(this.id == null || right.id == null)
			ret.setId(this.id + right.id)
		else
			ret.setId(this.id + right.id)
*/
		ret.setAmount(this.amount + right.amount)
		if(this.year == 0 || right.month == 0)
			ret.setMonth(this.year + right.year, this.month + right.month)
		else
			ret.setMonth(this.year, this.month)
		ret
	}

	def setCoef(c1: Double, c2: Double, c3: Double): Unit = {
		this.coef1 = c1
		this.coef2 = c2
		this.coef3 = c3
	}
	override def toString(): String = {
/*
		val linkStr = link match {
      case null => "(NULL)"
      case _ => link.toString()
    }
*/
    this.name + "," + this.id + "," + String.valueOf(this.price) + "," + String.valueOf(this.amount) + "\t," + this.coef1/(Math.sqrt(this.coef2*this.coef3)) + "\t" +String.valueOf(this.xx)  +"\t"+String.valueOf(this.yy) + "\tw: " + this.tem + "\t" +this.rain + ",!! " + this.coef1 + " " + this.coef2 + " " +this.coef3 
  }

}

object ReachabilityQuery extends java.io.Serializable {
    def main(args: Array[String]) {
			val DAY = 0
			val ID = 1
			val NAME = 2
			val UP = 5
			val MID = 6
			val DOWN = 7
			val AVG = 8
			val AMOUNT = 9
        val filePath = args(0)
        val weatherPath = args(1)
 //       val name = args(1)
//				val loop = new Breaks
        val outputPath = "final/crop"

        val conf = new SparkConf().setAppName("Reachability Query Application")
        val sc = new SparkContext(conf)

        // Cleanup output dir
        val hadoopConf = sc.hadoopConfiguration
        var hdfs = FileSystem.get(hadoopConf)
        try { hdfs.delete(new Path(outputPath), true) } catch { case _ : Throwable => { } }

        // Read input file
        val lines = sc.textFile(filePath, sc.defaultParallelism)
        val weatherLine = sc.textFile(weatherPath, sc.defaultParallelism)

				val wMap =  weatherLine.flatMap(x=> {
					val _word = x.split(",")
					val word = String.valueOf(Integer.valueOf(_word(0)) - 191100)
					var tmp = new Con(word)
					tmp.setIsAvg()
					tmp.setDay(Integer.valueOf(_word(1)))
					if(_word.length > 2 && _word(2)!="-")
						tmp.setWeather(Double.valueOf(_word(2)), Double.valueOf(_word(3)))
					Map(word -> tmp)
				})

				val r = wMap.collect()
        // Step 1: initial friend list, map each line in file to pairs
				val t = lines.flatMap (line => {
					val word = line.split(",")
					val value = new Con(word(NAME))
					val day = word(DAY).split("\\.")
					value.setPrice(Double.valueOf(word(AVG)))
					value.setId(word(ID))
					value.setAmount(Double.valueOf(word(AMOUNT)))
					value.setMonth(Integer.valueOf(day(0)), Integer.valueOf(day(1)))
					value.setDay(Integer.valueOf(day(2)))
					value.setPCount(1)
					Map(day(0) + day(1) + "." + word(ID) -> value)
				}).reduceByKey(_++_)
					.map(n=>{
						n._2.setPrice(n._2.getPrice()/n._2.getPCount())
						(n._1, n._2)
					}).cache

				val avg = t.map(n => {
					val word = n._1.split("\\.")
					n._2.setPCount(1)
					(word(0), n._2)
				}).union(wMap).reduceByKey(_+_)
					.map(n=>{
						n._2.setPrice(n._2.getPrice()/n._2.getPCount())
						n._2.setWeather(n._2.getTem()/n._2.getWCount(),n._2.getRain()/n._2.getWCount())
						(n._1, n._2)
					}).cache

				var avgW = avg.collect()
				val res1 = t.map(n => {
					val word = n._1.split("\\.")
					var index: Int = 0
					var count: Int = 0
					for (i <- avgW)	{
						if(i._1 == word(0)) index = count
						count += 1
					}
					count = 0
					var index2:Int = 0
					for (i <- r)	{
						if(i._1 == word(0) && i._2.day == n._2.day) index2 = count
						count += 1
					}
					n._2.setY(r(index2)._2.getTem() - avgW(index)._2.getTem())
					n._2.setX(n._2.getPrice() - avgW(index)._2.getPrice())
					(n._1, n._2)
				})
				val res2 = res1.map(n => {
					val word = n._1.split("\\.")
					(word(1), n._2)
				}).reduceByKey(_+++_).cache

        val retMap = res2.sortBy ({n => 
					(-Math.abs(n._2.coef1/(Math.sqrt(n._2.coef2*n._2.coef3))), n._1)
        })
//				.filter(n => Math.abs(n._2.coef1/(Math.sqrt(n._2.coef2*n._2.coef3))) < 0.3)
        // Output: count() and saveAsTextFile()
//        res.sortBy(_.toString).saveAsTextFile(outputPath)
//				println("ffffffffffff"+ list)
				
//				list.keys.foreach(x=> println("k: " + x))
//        ret.saveAsTextFile(outputPath)
/*
        val retMap = graph.sortBy {n => 
					(-n._2.getRank(), n._1)
        }
        val ret = retMap.map(n => 
          String.valueOf(n._1) + "\t" + n._2.toString()
        )
        ret.saveAsTextFile(outputPath)
*/
        retMap.saveAsTextFile(outputPath)
//        avg.saveAsTextFile(outputPath)
        sc.stop
    }
}

