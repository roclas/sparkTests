import org.apache.spark.SparkContext._
import org.apache.spark._

object hw{
  def main(args: Array[String]) {
    
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
      //.setJars(Seq("/home/carlos/proyectos/lastFmTest/target/scala-2.10/lastfmtest_2.10-1.0.jar"))
      .set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)

    val file="/home/carlos/proyectos/sparkTests/lastFmTest/data/lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv"
    val outputfile="/tmp/C"
    val songsRDD=sc.textFile(file)

    /*
    //number of times the user has played a song:
    //songsRDD.map(_.split("\t")).map(a=>(a(0),a(3)+a(5))).groupByKey().mapValues(_.toSet.size).collect()
    //or even better (more efficient, because groupByKey is inneficient)
    val result1 = songsRDD.map(_.split("\t")).flatMap(a=>
      a.length match{
        case 6=>Some(a(0),"%s|%s".format(a(3), a(5)))
        case _=>None //ignore lines with bad format
      }
    ).aggregateByKey(Set[String]())((s,v)=>s+v,(s1,s2)=>s1++s2).
      mapValues(_.size)

    //In a similar way, to take the top 100 played songs, we would do:
    val result2=songsRDD.map(_.split("\t")).flatMap(a=>
       a.length match{
        case 6=>Some("%s|%s".format(a(3), a(5)),1)
        case _=>None //ignore lines with bad format
      }
    ).aggregateByKey(0)((a,v)=>a+v,(s1,s2)=>s1+s2).
      map(a=>(a._2,a._1)).sortByKey(ascending=false).
      take(100)
    */

    //for the last question, we need to define some functions

    def getNLongestSeries (num:Int)(lo:scala.collection.immutable.SortedSet[(Long,String)]): List[(Long, Long, Long)] ={
      val r=((List[(Long,Long,Long)](),(0L,0L,0L)) /: lo ) (
       (ac,n)=>(
        if(ac._2._1==0)
          (ac._1, (n._1,n._1,0) )
        else if(n._1-1000*60*20 <= ac._2._2)
          (ac._1, (ac._2._1,n._1,n._1-ac._2._1) )
        else
          ( (ac._2 +: ac._1).sortBy(_._3).reverse.take(num), (n._1,n._1,0) )
       )
      )
      (r._2 +: r._1).sortBy(_._3).reverse.take(num)
    }

    val format=new java.text.SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")
    def getTime(s:String)=format.parse(s).getTime()

    val songsWithTimesRDD= songsRDD.map(_.split("\t")).flatMap(a=>
      a.length match{
        case 6=>Some(a(0),(getTime(a(1)), "%s|%s".format(a(3),a(5))))
        case _=>None //ignore lines with bad format
      }
    )

    val superlist =songsWithTimesRDD.aggregateByKey(
      scala.collection.immutable.SortedSet.empty[(Long,String)]( (Ordering.by[(Long, String), Long](_._1)) )
    ) ((s,v)=>s+v,(s1,s2)=>s1++s2) // a sorted set of songs(time,songName) by each key

    //superlist.cache() //we will use it twice in the future

    val superlist2 =superlist.map(a=>(getNLongestSeries(10)(a._2).map(b=>(a._1,b)))).flatMap(list=>list)

    /*
    val result3 =superlist.sortBy(_._2._3,false).take(10) // be too much
    */
    val partialResult3=superlist2.aggregate(List[(String, (Long, Long, Long))]())(
      (list,n)=>(n +: list).sortBy(_._2._3)(Ordering[Long].reverse).take(10),
      (l1,l2)=>(l1++l2).sortBy(_._2._3)(Ordering[Long].reverse).take(10)
    ).take(10)


    //val searchSongsNameRDD=songsWithTimesRDD.map(a=>( (a._1,a._2._1),a._2._2 ) )
    //partialResult3.foreach(println(_))
    //superlist.take(10).foreach(println(_))
    sc.parallelize(partialResult3).join(superlist).map(l=>
      "%s %s %s: %s".format(l._1,
        format.format(l._2._1._1),
        format.format(l._2._1._2),
        l._2._2.filter(a=>a._1>=l._2._1._1 && a._1<=l._2._1._2).map(x=>x._2).toList)
    ).saveAsTextFile(outputfile)
      //.foreach(println(_))



  }
  
}