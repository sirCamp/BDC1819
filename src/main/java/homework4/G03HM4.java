import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.time.temporal.ChronoUnit.MILLIS;

public class G03HM4
{
    public static void main(String[] args) throws Exception
    {

        //------- PARSING CMD LINE ------------
        // Parameters are:
        // <path to file>, k, L and iter

        if (args.length != 4) {
            System.err.println("USAGE: <filepath> k L iter");
            System.exit(1);
        }
        String inputPath = args[0];
        int k=0, L=0, iter=0;
        try
        {
            k = Integer.parseInt(args[1]);
            L = Integer.parseInt(args[2]);
            iter = Integer.parseInt(args[3]);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        if(k<=2 && L<=1 && iter <= 0)
        {
            System.err.println("Something wrong here...!");
            System.exit(1);
        }
        //------------------------------------
        final int k_fin = k;

        //------- DISABLE LOG MESSAGES
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //------- SETTING THE SPARK CONTEXT      
        SparkConf conf = new SparkConf(true).setAppName("kmedian new approach");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //------- PARSING INPUT FILE ------------
        JavaRDD<Vector> pointset = sc.textFile(args[0], L)
                .map(x-> strToVector(x))
                .repartition(L)
                .cache();
        long N = pointset.count();
        System.out.println("Number of points is : " + N);
        System.out.println("Number of clusters is : " + k);
        System.out.println("Number of parts is : " + L);
        System.out.println("Number of iterations is : " + iter);

        //------- SOLVING THE PROBLEM ------------
        double obj = MR_kmedian(pointset, k, L, iter);
        System.out.println("Objective function is : <" + obj + ">");
    }

    public static Double MR_kmedian(JavaRDD<Vector> pointset, int k, int L, int iter)
    {
        //
        // --- ADD INSTRUCTIONS TO TAKE AND PRINT TIMES OF ROUNDS 1, 2 and 3 
        //

        List<String> results = new ArrayList<>();

        //------------- ROUND 1 ---------------------------
        Instant instant = Instant.now();
        JavaRDD<Tuple2<Vector,Long>> coreset = pointset.mapPartitions(x ->
        {
            ArrayList<Vector> points = new ArrayList<>();
            ArrayList<Long> weights = new ArrayList<>();
            while (x.hasNext())
            {
                points.add(x.next());
                weights.add(1L);
            }
            ArrayList<Vector> centers = kmeansPP(points, weights, k, iter);
            ArrayList<Long> weight_centers = compute_weights(points, centers);
            ArrayList<Tuple2<Vector,Long>> c_w = new ArrayList<>();
            for(int i =0; i < centers.size(); ++i)
            {
                Tuple2<Vector, Long> entry = new Tuple2<>(centers.get(i), weight_centers.get(i));
                c_w.add(i,entry);
            }
            return c_w.iterator();
        });

        coreset.count();
        results.add(String.format("Execution for method ROUND 1 is: %s milliseconds",  MILLIS.between(instant, Instant.now())));

        //------------- ROUND 2 ---------------------------

        instant = Instant.now();
        ArrayList<Tuple2<Vector, Long>> elems = new ArrayList<>(k*L);
        elems.addAll(coreset.collect());
        ArrayList<Vector> coresetPoints = new ArrayList<>();
        ArrayList<Long> weights = new ArrayList<>();
        for(int i =0; i< elems.size(); ++i)
        {
            coresetPoints.add(i, elems.get(i)._1);
            weights.add(i, elems.get(i)._2);
        }

        ArrayList<Vector> centers = kmeansPP(coresetPoints, weights, k, iter);
        results.add(String.format("Execution for method ROUND 2 is: %s milliseconds",  MILLIS.between(instant, Instant.now())));

        //------------- ROUND 3: COMPUTE OBJ FUNCTION --------------------
        //
        //------------- ADD YOUR CODE HERE--------------------------------
        //
        instant = Instant.now();
        System.out.println(instant.toString());
        /*double sum = 0;
        for(int i = 0; i < pointset.count(); ++i)
        {
            double tmp = euclidean(pointset., centers.get(0));
            for(int j = 1; j < centers.size(); ++j)
            {
                if(euclidean(coresetPoints.get(i),centers.get(j)) < tmp)
                {
                    tmp = euclidean(coresetPoints.get(i), centers.get(j));
                }
            }
            sum += tmp;

        }*/

        final double result = pointset.glom()
                .mapToPair((PairFunction<List<Vector>, Double, Integer>) vectors -> {
                    //Tuple(avg, numberOfPoints used)
                    return new Tuple2<>(kmeansObj(vectors, centers), vectors.size());
                })
                //aggregate all avg
                .reduce((Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>) (acc, value) -> {
                    final int numberOfPoints = acc._2 + value._2;
                    return new Tuple2<>( (acc._1 * acc._2 + value._1 * value._2) / numberOfPoints , numberOfPoints);
                })._1;


        results.add(String.format("Execution for method ROUND 3 is: %s milliseconds",  MILLIS.between(instant, Instant.now())));
        System.out.println(Instant.now().toString());
        //double final_res = sum/coresetPoints.size();
        results.forEach(x -> System.out.println(x));

        return result;


    }

    // return the minimum distance between a point p and a set of points S
    public static double nearest_distance_set(Vector p, ArrayList<Vector> S) {
        double dist = 0.0;
        double min = compute_distance(p, S.get(0));

        if (S.size()>1)
            for(int i=1; i<S.size(); i++) {
                dist = compute_distance(p, S.get(i));
                if (dist<min)
                    min = dist;
            }

        return min;
    }

    /*
     kmeansObj: must compute the sum of the distances (NOT SQUARED DISTANCES) of each point to the
     closest center, and then divide this sum by the total number of points of P, so to get the average distance.
    */
    public static double kmeansObj(List<Vector> P, ArrayList<Vector> C) {
        double sum = 0.0;
        double val;
        for(int i=0; i<P.size(); i++) {
            val = nearest_distance_set(P.get(i), C);
            sum += val;
        }

        return sum/(double)P.size();
    }


    public static ArrayList<Long> compute_weights(ArrayList<Vector> points, ArrayList<Vector> centers)
    {
        Long weights[] = new Long[centers.size()];
        Arrays.fill(weights, 0L);
        for(int i =0; i < points.size(); ++i)
        {
            double tmp = euclidean(points.get(i), centers.get(0));
            int mycenter = 0;
            for(int j = 1; j < centers.size(); ++j)
            {
                if(euclidean(points.get(i),centers.get(j)) < tmp)
                {
                    mycenter = j;
                    tmp = euclidean(points.get(i), centers.get(j));
                }
            }
            weights[mycenter] += 1L;
        }
        ArrayList<Long> fin_weights = new ArrayList<>(Arrays.asList(weights));
        return fin_weights;
    }

    public static Vector strToVector(String str) {
        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

    // Euclidean distance
    public static double euclidean(Vector a, Vector b) {
        return Math.sqrt(Vectors.sqdist(a, b));
    }

    // compute distance between two points
    public static double compute_distance(Vector x, Vector y) {
        //System.out.println("sqrt " + Math.sqrt(Vectors.sqdist(x, y)));
        return Math.sqrt(Vectors.sqdist(x, y));
    }

    // return the minimum distance between a point p and a set of points S
    // and the index of the point in S
    public static double[] nearest_distance_k(Vector p, ArrayList<Vector> S) {
        // distanza[0], index[1]
        double[] dist_k = new double[2];
        double dist;
        dist_k[0] = compute_distance(p, S.get(0));
        dist_k[1] = 0;

        if (S.size()>1) {
            for(int i=1; i<S.size(); i++) {
                dist = compute_distance(p, S.get(i));
                if (dist<dist_k[0]) {
                    dist_k[0] = dist;
                    dist_k[1] = i;
                }
            }
        }
        return dist_k;
    }

    // compute kmeans++ to select the initial set of points C'
    public static ArrayList<Vector> kmeansPP(ArrayList<Vector> P, ArrayList<Long> WP, int k, int iter) {
        ArrayList<Vector> C = new ArrayList<>(k); // C'
        ArrayList<Vector> P_C = new ArrayList<>(P); // set P-C'
        ArrayList<Long> WP_C = new ArrayList<>(WP); // weights of points in P-C'
        int point_lenght = P.get(0).size();
        double weight_dist, new_dist, current_dist, min;

        // obtain a random number between [0 and |P|).
        int pos = (int)(Math.random()*P.size());

        // choose a random point from P and add it to C'
        Vector c1 = P.get(pos);
        C.add(c1);

        // remove c1 from P-C'
        P_C.remove(pos);
        WP_C.remove(pos);

        ArrayList<Double> P_C_min_dist = new ArrayList<>(P_C.size());

        for(int i=0; i<k-1; i++) {
            ArrayList<Double> min_weight_dist = new ArrayList<>(P_C.size());
            double sum_weighted_dist=0.0;
            for(int q=0; q<P_C.size(); q++) {
                if (i==0) {
                    // compute distance and weighted distance between each point in P-C'
                    // and the first random point of C'
                    new_dist = compute_distance(P_C.get(q), c1);
                    P_C_min_dist.add(new_dist);
                    weight_dist = (double)WP_C.get(q)*new_dist;
                }
                else {
                    // compute distance between each point in P-C' and the new random point of C'
                    new_dist = compute_distance(P_C.get(q), C.get(i));
                    current_dist = P_C_min_dist.get(q);
                    // if new distance is less than the current distance
                    // update min distance and compute weighted distance
                    if (new_dist < current_dist) {
                        P_C_min_dist.set(q, new_dist);
                        weight_dist = (double)WP_C.get(q)*new_dist;
                    }
                    else {
                        weight_dist = (double)WP_C.get(q)*current_dist;
                    }
                }

                min_weight_dist.add(weight_dist);
                sum_weighted_dist += weight_dist; // to compute probabilities
            }

            // compute probs
            ArrayList<Double> probs = new ArrayList<>(P_C.size());
            for(int q=0; q<P_C.size(); q++)
                probs.add(min_weight_dist.get(q)/sum_weighted_dist);

            // compute cumulative probs
            ArrayList<Double> cumprobs = new ArrayList<>(P_C.size());
            cumprobs.add(probs.get(0));
            for(int q=1; q<P_C.size(); q++) {
                cumprobs.add(cumprobs.get(q-1) + probs.get(q));
            }

            double r = Math.random();

            // take new point for C'
            for (int q=0; q<P_C.size(); q++) {
                if (r<cumprobs.get(q)) {
                    pos = q;
                    break;
                }
            }

            // add new point to C'
            C.add(P_C.get(pos));
            // remove from P-C'
            P_C.remove(pos);
            WP_C.remove(pos);
            P_C_min_dist.remove(pos);
        }

        if (iter==0)
            return C;
        else {
            // Lloyd's iterations
            double[] dist_k = new double[2];
            dist_k[0] = 0.0;
            dist_k[1] = 0.0;
            ArrayList<double[]> min_distances_k = new ArrayList<>(P.size());
            for(int i=0; i<P.size(); i++) min_distances_k.add(dist_k);

            ArrayList<Vector> centroids = new ArrayList<>();
            for (int it=0; it<iter; it++) {
                // to keep the dimension of each partition
                int[] num_points_centroid = new int[k];
                for (int i=0; i<k; i++)
                    num_points_centroid[i] = 0;

                // compute distance and partition for each point in P
                for (int p=0; p<P.size(); p++) {
                    if (it==0)
                        min_distances_k.set(p, nearest_distance_k(P.get(p), C));
                    else
                        min_distances_k.set(p, nearest_distance_k(P.get(p), centroids));
                }

                // compute centroids
                // define new centroids set (C) with initial k vectors of zeros
                centroids.clear();
                for (int i=0; i<k; i++)
                    centroids.add(Vectors.zeros(point_lenght));
                for (int p=0; p<P.size(); p++) {
                    Vector tmp_p = P.get(p); // point p
                    double w = WP.get(p); // weight of p
                    dist_k = min_distances_k.get(p);
                    int num_part = (int)dist_k[1]; // retrieve partition number of p
                    Vector tmp_c = centroids.get(num_part); // retrieve centroid of partition num_part
                    BLAS.axpy(w, tmp_p, tmp_c); // tmp_c += w*tmp_p
                    centroids.set(num_part, tmp_c); // update centroid
                    num_points_centroid[num_part] += w; // update partition's number points
                }

                for (int c=0; c<k; c++) {
                    Vector tmp_c = centroids.get(c); // retrieve centroid
                    double div = 1.0 / (double) num_points_centroid[c];
                    // sum of weighted points divided by partition's number points
                    BLAS.scal(div, tmp_c);
                    centroids.set(c, tmp_c);
                }
            }
            return centroids;
        }
    }
}
