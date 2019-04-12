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

        /**
         * this isImproved Word count 1 algorithm described in class the using reduceByKey method to compute the final counts
         */
        JavaPairRDD<String, Long> wordCount1 = docs
                // Map phase: each document is mapped to its list of words paired with their occurencies: (w, ci(w))
                .flatMapToPair((PairFlatMapFunction<String, String, Long>) document -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    Arrays.stream(tokens).forEach(token -> {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    });
                    counts.forEach((key, value) -> {
                        pairs.add(new Tuple2<String, Long>(key, value));
                    });

                    return pairs.iterator();

                })//Reduce phase: for each pair (w,ci(w)) where w is the same word, we reduce it to (w, c(w)) where w is the analyzed word and c(w) corresponds to sum of all eht occurencies of w in each document.
                // In other words, c(w) is equal to the summatory of all ci(w) by key w
                .reduceByKey(reduceFunction);

        wordCounts.add(wordCount1);

        /**
         * This is the Word count 2a:
         * a variant of the algorithm presented in class where random keys take K possible values, where K is the value given in input.
         * The easiest thing to do is to assing random integers between 0 and K-1. The algorithm presented in class had K=N^(1/2).
         * You must use method groupBy to assign random keys to pairs and to group the pairs based on the assigned keys as required in the first round of the algorithm.
         */
        JavaPairRDD<String, Long> wordCount2a = docs
                // Map phase: each document is mapped to its list of words paired with their occurencies: (w, ci(w))
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                    Arrays.stream(tokens).forEach(token -> {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    });
                    counts.forEach((key, value) -> {
                        pairs.add(new Tuple2<String, Long>(key, value));
                    });
                    return pairs.iterator();
                })
                .groupBy(randomKAssignment) // Using the group by we group each (w,ci(w)) by a random key k € [0, K-1] where K is given in input.

                /**
                 *  For all (x,(w,ci(w))), that have been generated in the previous step, of group x, we create a (w,c(x,w)) for each word w.
                 *  where c(x,w) corresponds to the summatory of (x,(w,ci(w))).
                 *  */
                .flatMapToPair((PairFlatMapFunction<Tuple2<Integer, Iterable<Tuple2<String, Long>>>, String, Long>) element -> {
                    /**for the each element we create its list of pairs (w, c(w))
                     *
                     * Then we iterate over the list abdm for each word of the list we sum the occurencies of the same word
                     * and we add a the tuple (w, c(w)) where c(w) is the sum of all the occurencies in the documents
                     */
                    final List<Tuple2<String, Long>> list = new ArrayList<>();
                    StreamSupport.stream(element._2.spliterator(), false).collect(groupingBy(t -> t._1)).forEach((k, v) -> {
                        list.add(v.stream().reduce((t1, t2) -> new Tuple2<String, Long>(k, t1._2 + t2._2)).orElse(new Tuple2<String, Long>("", 0L)));
                    });

                    return list.iterator();
                })//Reduce phase: for each pair (w,ci(w)) where w is the same word, we reduce it to (w, c(w)) where w is the analyzed word and c(w) corresponds to sum of all eht occurencies of w in each document.
                // In other words, c(w) is equal to the summatory of all ci(w) by key w
                .reduceByKey(reduceFunction);

        wordCounts.add(wordCount2a);

        /**
         * This is the Word count 2b:
         * a variant that does not explicitly assign random keys but exploits the subdivision of docs into K parts in combination with mapPartitionToPair to access each partition separately.
         * Again, K is the value given in input. Note that if docs was initially partitioned into K parts, then, even after transformations that act on individual elements,
         * the resulting RDD stays partitioned into K parts and you may exploit this partition. However, you can also invoke repartition(K) to reshuffle the RDD elements at random.
         * Do whatever you think it yields better performance.
         */
        JavaPairRDD<String, Long> wordCount2b = docs
                // Map phase: each document is mapped to its list of words paired with their occurencies: (w, ci(w))
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    Arrays.stream(tokens).forEach(token -> {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    });
                    counts.forEach((key, value) -> {
                        pairs.add(new Tuple2<String, Long>(key, value));
                    });
                    return pairs.iterator();

                }) /**
         * Similar to the varian a, for all (x,(w,ci(w))), that have been generated in the previous step, of group x, we create a (w,c(x,w)) for each word w.
         *  where c(x,w) corresponds to the summatory of (x,(w,ci(w))).
         **/
                .mapPartitionsToPair(tuple2Iterator -> {

                    //for the each element we create its list of pairs (w, c(w))
                    Iterable<Tuple2<String, Long>> splitIterator = () -> tuple2Iterator;
                    // parrallel falso in order to keep the order and no concurrecies
                    Map<String, List<Tuple2<String, Long>>> groupedTuple = StreamSupport.stream(splitIterator.spliterator(), false)
                            .collect(groupingBy(t -> t._1));
                    final List<Tuple2<String, Long>> list = new ArrayList<>();

                    groupedTuple.forEach((k, v) -> {
                        Tuple2 tmpTuple = v.stream().reduce((tuple1, tuple2) -> new Tuple2<>(k, tuple1._2 + tuple2._2)).orElse(new Tuple2<String, Long>("", 0L));
                        if (tmpTuple != null) {
                            list.add(tmpTuple);
                        }
                    });
                    return list.iterator();

                })//Reduce phase: for each pair (w,ci(w)) where w is the same word, we reduce it to (w, c(w)) where w is the analyzed word and c(w) corresponds to sum of all eht occurencies of w in each document.
                // In other words, c(w) is equal to the summatory of all ci(w) by key w
                .reduceByKey(reduceFunction);

        wordCounts.add(wordCount2b);
        List<String> results = new ArrayList();
        for (int i = 0; i < wordCounts.size(); i++) {
            JavaPairRDD<String, Long> wc = wordCounts.get(i);
            Instant instant = Instant.now();
            //due to the lazy execution approaches of Spark, we have to trigger the computation in order to measure the real execution timing
            wc.count();
            results.add(String.format("Execution for method %d is: %s milliseconds", i + 1, MILLIS.between(instant, Instant.now())));
            double averageLenght = wc.keys().map(word -> word.length()).reduce((x, y) -> x + y) / (double) wc.count();
            results.add(String.format("Average length for method %d is: %f", i + 1, averageLenght));
        }

        // we iterate over the string results and print them (both timing and average length)
        results.forEach(result -> System.out.println(result));

        /**
         * FINAL CONSIDERATIONS:
         * + Using repartition too frequently is not always useful. It could introduces overhead, specially when the Spark context is deployed on a cluster with sever workers (even worse when those workers are have different computation power, or when
         * the network performances between the nodes are not optimal)
         *
         * + Repartition seems to increase the performances, but the number of partitions seems to be correlated to the number of the workers and by dataset dimension: i.e: having 12 cores on our machines,
         * K repartitions with K = 12 have better performances than K = 100
         *
         * + Performances are also correlated to lambda functions that are used by each step: Using Java 8 statements, like stream or reduce, can optimize the execution, computation time and reduce the algorithm complexity.
         */

    }
}
