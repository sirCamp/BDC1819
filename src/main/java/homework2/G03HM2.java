package homework2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.time.Instant;
import java.util.*;
import java.util.stream.StreamSupport;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.stream.Collectors.groupingBy;


/*interface CommonMethodsInterface extends Serializable{

    default Iterator<Tuple2<String, Long>> tuplesOfWordsAndTheirOccurrences(String document){
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
        /*return pairs.iterator();
    }

    public static Long reduceToSum(Long a, Long b){
        return a+b;
    }
}*/
@SuppressWarnings("unchecked")
public class G03HM2 /*implements CommonMethodsInterfac*/ {


    /*private static Function tuplesOfWordsAndTheirOccurrences = new Function() {
        public Iterator<Tuple2<String, Long>> call(String document){
            String[] tokens = document.split(" ");
            HashMap<String, Long> counts = new HashMap<>();
            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
            for (String token : tokens) {
                counts.put(token, 1L + counts.getOrDefault(token, 0L));
            }
            for (Map.Entry<String, Long> e : counts.entrySet()) {
                pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
            }

            return pairs.iterator();
        }

    } ;  */


    public static void main(String[] args) throws FileNotFoundException {

        if (args.length < 2) {
            throw new IllegalArgumentException("Expecting the file name and the number of partitions on the command line");
        }

        final int K = Integer.valueOf(args[0]).intValue();
        String textFilePath = args[1];
        final Random random = new Random();

        Function2 reduceFunction = new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }

        };

        Function randomKAssignment = new Function<Tuple2<String, Long>, Integer>() {
            @Override
            public Integer call(Tuple2<String, Long> v1) throws Exception {
                return random.nextInt(K);
            }
        };

        // Setup Spark
        SparkConf conf = new SparkConf(true)
                .setAppName("Second template")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a parallel collection
        JavaRDD<String> docs = sc.textFile(textFilePath);

        //repartition the documents in K parts
        docs = docs.repartition(K).cache();

        //due to lazy approach of Spark, make an operation on the RDD activate the loading of the file
        System.out.println("Number of documents: " + String.valueOf(docs.count()));


        List<JavaPairRDD<String, Long>> wordCounts = new ArrayList<>();

        JavaPairRDD<String, Long> wordCount1 = docs
                // Map phase
                .flatMapToPair((PairFlatMapFunction<String, String, Long>) document -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }

                    return pairs.iterator();

                })//reduce phase
                .reduceByKey(reduceFunction);

        wordCounts.add(wordCount1);


        JavaPairRDD<String, Long> wordCount2a = docs
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
                    return pairs.iterator();
                })
                .groupBy(randomKAssignment)
                .flatMapToPair((PairFlatMapFunction<Tuple2<Integer, Iterable<Tuple2<String, Long>>>, String, Long>) element -> {
                    //for the each element we create its list of pairs (w, c(w))
                    Map<String, List<Tuple2<String, Long>>> groupedTuple = StreamSupport.stream(element._2.spliterator(), false)
                            .collect(groupingBy(t -> t._1));

                    /**
                     * then we iterate over the list abdm for each word of the list we sum the occurencies of the same word
                     * and we add a the tuple (w, c(w)) where c(w) is the sum of all the occurencies in the documents
                     */
                    final List<Tuple2<String, Long>> list = new ArrayList<>();
                    groupedTuple.forEach((k, v) -> {
                        Tuple2 tmpTuple = v.stream().reduce((t1, t2) -> new Tuple2<String, Long>(k, t1._2 + t2._2)).orElse(null);
                        if (tmpTuple != null) {
                            list.add(tmpTuple);
                        }

                    });

                    return list.iterator();
                })//round2
                .reduceByKey(reduceFunction);

        wordCounts.add(wordCount2a);

        JavaPairRDD<String, Long> wordCount2b = docs
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
                    return pairs.iterator();
                }).mapPartitionsToPair(tuple2Iterator -> {

                    //for the each element we create its list of pairs (w, c(w))
                    Iterable<Tuple2<String, Long>> splitIterator = () -> tuple2Iterator;
                    // parrallel falso in order to keep the order and no concurrecies
                    Map<String, List<Tuple2<String, Long>>> groupedTuple = StreamSupport.stream(splitIterator.spliterator(), false)
                            .collect(groupingBy(t -> t._1));
                    final List<Tuple2<String, Long>> list = new ArrayList<>();

                    groupedTuple.forEach((k, v) -> {
                        Tuple2 tmpTuple = v.stream().reduce((tuple1, tuple2) -> new Tuple2<>(k, tuple1._2 + tuple2._2)).orElse(null);
                        if (tmpTuple != null) {
                            list.add(tmpTuple);
                        }
                    });
                    return list.iterator();

                })
                .reduceByKey(reduceFunction);

        wordCounts.add(wordCount2b);
        List<String> results = new ArrayList();
        for (int i = 0; i < wordCounts.size(); i++) {
            JavaPairRDD<String, Long> wc = wordCounts.get(i);
            Instant instant = Instant.now();
            wc.count(); // due to execute all (lazy)
            results.add(String.format("Execution for method %d is: %s milliseconds", i + 1, MILLIS.between(instant, Instant.now())));
            double averageLenght = wc.keys().map(word -> word.length()).reduce((x, y) -> x + y) / (double) wc.count();
            results.add(String.format("Average length for method %d is: %f", i + 1, averageLenght));
        }

        results.forEach(result -> System.out.println(result));

    }
}
