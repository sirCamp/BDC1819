import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Scanner;

public class G03HM1 {

    public static void main(String[] args) throws FileNotFoundException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }

        ArrayList<Double> lNumbers = new ArrayList<>();
        Scanner s =  new Scanner(new File(args[0]));
        while (s.hasNext()){
            lNumbers.add(Double.parseDouble(s.next()));
        }
        s.close();

        // Setup Spark
        SparkConf conf = new SparkConf(true)
                .setAppName("Preliminaries");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a parallel collection
        JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);

        /**
         * STEP 1 getting max values in two ways
         */
        System.out.println("Max number is: "+String.valueOf(dNumbers.reduce((x, y) -> {return (x >= y?x:y); })));
        System.out.println("Max number is: "+String.valueOf(dNumbers.max(new DoubleComparator())));


        /**
         * STEP 2 getting a normalized version of dNumbers (0,1)
         * normalized = (x-min(x))/(max(x)-min(x))
         *
         */
        final Double max = dNumbers.max(new DoubleComparator());
        final Double min = dNumbers.min(new DoubleComparator());

        JavaRDD<Double> dNormalizedNumbers = dNumbers.map((x) -> (x-min)/(max-min));
        //dNormalizedNumbers.collect().stream().forEach(x -> System.out.println(x)); --> TODO: de-comment if you want to see the normalization


        /**
         * STEP 3 getting a "statistics of our choiche over the dNomralized data
         * median:
         * mean:
         *
         */

        //Mediana
        dNormalizedNumbers = dNormalizedNumbers.sortBy(x -> x, true, dNormalizedNumbers.partitions().size());
        long numberOfElements = dNormalizedNumbers.count();
        final long medianIndex = numberOfElements % 2 == 0? numberOfElements / 2: numberOfElements+1 /2;
        System.out.println("Median of the values: "+String.valueOf(dNormalizedNumbers.zipWithIndex().filter(tuple -> tuple._2 == medianIndex).first()._1()));

        //Media

    }

    // It is important to mark this class as `static`.
    public static class DoubleComparator implements Serializable, Comparator<Double> {

        public int compare(Double a, Double b) {

            if (a < b) return -1;
            else if (a > b) return 1;
            return 0;
        }

    }
}
