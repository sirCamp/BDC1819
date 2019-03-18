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
            try {
                double val = Double.parseDouble(s.next());
                // Add 'val' to the ArrayList only if is non-negative
                if (val >= 0) {
                    lNumbers.add(val);
                }
            }
            catch (Exception e){
                System.out.println("The element trying to add is not a number");
            }
        }
        s.close();

        if(lNumbers.isEmpty()){
            throw new IllegalArgumentException("To deal with this assigment you need to have at least one element");
        }

        DoubleComparator comparator = new DoubleComparator();

        // Setup Spark
        SparkConf conf = new SparkConf(true)
                .setAppName("Preliminaries");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a parallel collection
        JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);

        /**
         * STEP 1 get the max value in two ways:
         * In first way we obtain the max value through the usage of the reduce function that takes a lambda function as argument. The lambda function compares two elements and returns the biggest one.
         * In the second method we used the max method of RDD interface with a custom Comparator that compare two numbers and that is used by the max function to compute the max value.
         */

        /**
         * This value is the max value calculated in the first way as previously described
         */
        System.out.println("Max number is: "+String.valueOf(dNumbers.reduce((x, y) -> {return (x >= y?x:y); })));

        /**
         * This value is the max value calculated in the second way as previously described
         */
        System.out.println("Max number is: "+String.valueOf(dNumbers.max(comparator)));


        /**
         * STEP 2 get a normalized version of dNumbers (0,1)
         * For this step of the assignment, we have used the map function  of the RDD interface, that it takes as argument a lambda function that calculate the normlized version of the current element of dataset.
         * The normalization function is equal to x-min/max-min, where x is the element of the dataset that have to be normalized, min is the minimal value of dataset and max is the maximum.
         * Thus, before to call the map function, we have used min and max method of the RDD interface to retrive the minimal and maximal value of the whole dataset.
         *
         */
        final Double lMax = dNumbers.max(comparator);
        final Double lMin = dNumbers.min(comparator);

        JavaRDD<Double> dNormalizedNumbers = dNumbers.map((x) -> (x-lMin)/(lMax-lMin));
        /**
         * These values are the normalized values
         */
        dNormalizedNumbers.collect().stream().forEach(x -> System.out.println(x));


        /**
         * STEP 3 get statistics of our choiche over the dNomralized data
         *
         * As statistics, we have choose the median and the mean.
         * In order to work with the first we have use four methods of the RDD interface:
         * + sortBy: necessary to sort the elements
         * + zipWithIndex: necessary to map each element of the dataset to a tuple that contains the element and its index in the dataset.
         * + count: necessary to count the number of elements in the dataset.
         * + filter: necessary to filters the element by their index.
         *
         * Once we have ordered and zipped the elements  with their index, we have calculated the index of the median by counting  the elements and by checking if the number of elements was odd or even.
         * After that we have filtered the element by checking if their index was equal to the median index. At this point we have used the first method to retrieve the number with the position equals to the index of the median.
         * N.B: The real index of the median is equal to lMedianIndex-1 since the first index of the zipped RDD is 0 instead of 1 in agreement with the fact that in computer science 0 is a number and not just a representation.
         *
         * About the mean, we have used a reduce function that take care about the sum of all elements of the dataset.
         * Then, we have divided the results for the number of the elements that have been calculated through the usage of the count method of the RDD interface.
         */

        //Median

        dNormalizedNumbers = dNormalizedNumbers.sortBy(x -> x, true, dNormalizedNumbers.partitions().size());
        long lNumberOfElements = dNormalizedNumbers.count();
        final long lMedianIndex = lNumberOfElements % 2 == 0? lNumberOfElements / 2: lNumberOfElements+1 /2;

        /**
         * This value is the median value
         */
        System.out.println("The median of the normalized values is: "+String.valueOf(dNormalizedNumbers.zipWithIndex().filter(tuple -> tuple._2 == lMedianIndex-1).first()._1()));

        //Mean
        double lMean = (dNormalizedNumbers.reduce((x,y) -> x+y)) / dNormalizedNumbers.count();
        /**
         * This value is the mean value
         */
        System.out.println("The mean of the normalized values is: " + lMean);
    }


    public static class DoubleComparator implements Serializable, Comparator<Double> {

        public int compare(Double a, Double b) {

            if (a < b) return -1;
            else if (a > b) return 1;
            return 0;
        }

    }
}
