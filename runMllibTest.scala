#!/bin/sh
classpath=$(find mlibTest/ -name "*.jar"|grep -v sources|grep -v javadoc| awk '{a[$0]=$0}END{for(i in a){printf("%s;",i);}}')
echo $classpath
exec scala -classpath 'mlibTest/target/scala-2.11/lastfmtest_2.11-1.0.jar' "$0" "$@"
!#
object HelloWorld extends App {
  import Helper
  val result=Helper.getNLongestSeries(3)(List(1,2,3,4,5,7,9,10,11,12,13));
  println("Hello, world!")
}
HelloWorld.main(args)
