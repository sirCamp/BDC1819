package homework2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.groupingBy;

public class SecondTemplate {

    public static void main(String[] args) throws FileNotFoundException {
        if (args.length < 2) {
            throw new IllegalArgumentException("Expecting the file name and the number of partitions on the command line");
        }

        final int K = Integer.valueOf(args[0]).intValue();
        String textFilePath = args[1];

        // Setup Spark
        SparkConf conf = new SparkConf(true)
                .setAppName("Second template")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a parallel collection
        JavaRDD<String> docs = sc.textFile(textFilePath);

        //repartition the documents in K parts
        docs = docs.repartition(K);

        System.out.println("Numero di elementi: "+String.valueOf(docs.count()));
        long start = System.currentTimeMillis();
        JavaPairRDD<String, Long> wordcountpairs = docs
                // Map phase
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    /*for(String t: tokens){
                        pairs.add(new Tuple2<>(t, 1L));
                    }*/
                    return pairs.iterator();
                })
                .reduceByKey((x, y) -> x+y);
                // Reduce phase
                /*.groupByKey()
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });*/

        wordcountpairs.cache();
        wordcountpairs.foreach(tp -> {
            //System.out.println("Word '"+tp._1+"' is present: "+String.valueOf(tp._2)+" times");
        });
        long end = System.currentTimeMillis();
        System.out.println("Elapsed time " + (end - start) + " ms");



        final Random random = new Random();
        JavaPairRDD<String, Long> wordcountpairsSecondFirstVariant = docs
                // Map phase
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    /*for(String t: tokens){
                        pairs.add(new Tuple2<>(t, 1L));
                    }*/
                    return pairs.iterator();
                })
                .groupBy((Function<Tuple2<String, Long>, Integer>) v1 -> random.nextInt(K))
                /*.flatMapToPair((PairFlatMapFunction<Tuple2<Integer, Iterable<Tuple2<String, Long>>>, String, Long>) element -> {

                    Map<String, List<Tuple2<String, Long>>> map = new HashMap<>();
                    element._2.forEach();

                })*/

                .flatMapToPair((PairFlatMapFunction<Tuple2<Integer, Iterable<Tuple2<String, Long>>>, String, Long>) ele -> {
                    /*final Map<String,List<Tuple2<String,Long>>> map = StreamSupport.stream(ele._2.spliterator(), false)
                            .collect(groupingBy(t -> t._1));*/

                    Map<String,Long> map = new HashMap<>();
                    //Iterator<Tuple2<String,Long>>it = ele._2.iterator();
                    Collection<Tuple2> collection = new ArrayList<>();
                    ele._2.forEach(collection::add);

                    /*while(it.hasNext()){
                        collection.add(it.next());
                    }*/

                    Map<String, List<Tuple2<String,Long>>> groupedTuple = collection.stream().collect(groupingBy(tuple -> tuple._1));

                    //iterate over each list of string and reduce to list of tuple
                    final List<Tuple2<String, Long>> list =  new ArrayList<>();
                    groupedTuple.forEach( (k, v)-> {
                        Tuple2 tmpTuple = v.stream().reduce((t1, t2) -> new Tuple2<String,Long>(k, t1._2+t2._2)).orElse(null);
                        if(tmpTuple != null){
                            list.add(tmpTuple);
                        }

                    });

                    /*for (Map.Entry e:map.entrySet()) {

                        System.out.println("key: "+e.getKey());
                        for(Tuple2 t: (List<Tuple2>)e.getValue()){
                            System.out.println(String.valueOf(t._1)+" -> "+String.valueOf(t._2));
                        }

                    }*/
                    return list.iterator();
                    //return map.values().stream().map(tuple2s -> tuple2s.stream().reduce((stringIntegerTuple2, stringIntegerTuple22) -> new Tuple2<>(stringIntegerTuple2._1, stringIntegerTuple2._2 + stringIntegerTuple22._2)).orElse(null)).iterator();
                })//round2
                .reduceByKey((x, y) -> x+y);
        // Reduce phase
                /*.groupByKey()
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });*/


        JavaPairRDD<String, Long> wordcountpairsSecondSecondVariant = docs
                // Map phase
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    /*for(String t: tokens){
                        pairs.add(new Tuple2<>(t, 1L));
                    }*/
                    return pairs.iterator();
                }).mapPartitionsToPair()


        wordcountpairsSecondFirstVariant.cache();
        /*wordcountpairs.cache();
        wordcountpairs.foreach(tp -> {
           // System.out.println("Word '"+tp._1+"' is present: "+String.valueOf(tp._2)+" times");
        });*/
        //long end = System.currentTimeMillis();
        //System.out.println("Elapsed time " + (end - start) + " ms");
    }

}
