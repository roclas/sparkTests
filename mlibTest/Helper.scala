

object Helper{
    def getNLongestSeries(num:Int)(lo:List[Int])={
      val r=((List[(Int,Int,Int)](),(0,0,0)) /: lo) (
        (ac,n)=>(
          if (ac._2._1==0)
            (ac._1, (n,n,0) )
          else if(n-1 == ac._2._2)
            (ac._1, (ac._2._1,n,n-ac._2._1) )
          else
            ( (ac._2 +: ac._1).sortBy(_._3).reverse.take(num), (n,n,0) )
        )
      )
      (r._2 +: r._1).sortBy(_._3).reverse.take(num)
    }

    def getNLongestSeries (num:Int)(lo:scala.collection.immutable.SortedSet[(Long,String)])={
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

}