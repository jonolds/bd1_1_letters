import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class Letters extends SparkJon {
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		JavaRDD<String> lines = settings("letters").read().textFile("pg100.txt").javaRDD();
		
		JavaRDD<String> alpha_only = lines.map(ln->ln.replaceAll("[^a-zA-Z\\s]", "").replaceAll("\\s+", " "));
		
		JavaPairRDD<Character, Integer> first_char_1 = alpha_only.flatMapToPair(new PairFlatMapFunction<String, Character, Integer>() {
			public Iterator<Tuple2<Character, Integer>> call(String s) throws Exception {
				List<Tuple2<Character, Integer>> list = new ArrayList<>();
				Arrays.stream(s.split("\\s+")).filter(s1->s1.length() > 0).map(s2->new Tuple2<>(s2.toLowerCase().charAt(0), 1)).forEach(t->list.add(t));
				return list.iterator();
			}
			
		});
		
		JavaPairRDD<Character, Integer> counts = first_char_1.reduceByKey((k1, k2) -> k1 + k2).sortByKey();
		
		counts.saveAsTextFile("output/out2");

		counts.foreach(x->System.out.println(x._1 + " " + x._2));
		
		Thread.sleep(15000);
	}
}