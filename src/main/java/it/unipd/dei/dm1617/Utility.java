/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.rdd.RDD;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;

/**
 *
 * @author DavideDP
 */
public class Utility {

    //scrive nuovi centroidi dentro al clustering
    public static void initRandomCentroids(Clustering primeClustering, int k) {
        for (int i = 0; i <= k - 1; i++) {
            Point centroid = primeClustering.getRandom();
            primeClustering.getCenters().add(centroid);
        }
    }
    public static List<Vector> toArrayList(double[][] array )
    {
        LinkedList<Vector> rowsList = new LinkedList<Vector>();
        for (int i = 0; i < array.length; i++) {
          Vector currentRow = Vectors.dense(array[i]);
          rowsList.add(currentRow);
        }
        return rowsList;
    }
    public static ArrayList toArrayList(double[] dd) {
        ArrayList v = new ArrayList(dd.length);
        for (int i = 0; i < dd.length; i++) {
            v.add(dd[i]);
        }
        return v;
    }

    //posizione nell'array del min e il riferimento all'oggetto
    public static Tuple2<Integer, Point> mostClose(ArrayList<Point> P, Point c) {
        Point p = null;
        double min = Double.MAX_VALUE;
        int argmin = -1;
        for (int i = 0; i < P.size(); i++) {
            double dist = Distance.calculateDistance(P.get(i).parseVector(), c.parseVector(), "standard");
            if (dist <= min && P.get(i).equals(p) == false) {
                p = P.get(i);
                min = dist;
                argmin = i;
            }
        }
        return new Tuple2(argmin, p);
    }

    public static Tuple2<Integer, Point> mostClose(ArrayList<Point> P, ArrayList<Point> S) {
        Point p = null;
        double min = Double.MAX_VALUE;
        int argmin = -1;
        for (int i = 0; i < S.size(); i++) {
            Tuple2<Integer, Point> t = Utility.mostClose(P, S.get(i));
            double dist = Distance.calculateDistance(t._2.parseVector(), S.get(i).parseVector(), "standard");
            if (dist <= min && P.get(i).equals(p) == false) {
                p = S.get(i);
                min = dist;
                argmin = i;
            }
        }
        return new Tuple2(argmin, p);
    }

    public static ArrayList<Point> getRandomCenters(int dim, int k) {
        ArrayList<Point> S = new ArrayList<Point>(k);
        for (int i = 0; i <= k - 1; i++) {
            S.add(new PointCentroid(Utility.toArrayList(Evaluation.generateRandomVector(dim).toArray())));
        }
        return S;
    }

    public static Tuple2<Integer, Point> mostFar(ArrayList<Point> P, Point c) {
        Point p = null;
        double max = Double.MIN_VALUE;
        int argmax = -1;
        for (int i = 0; i < P.size(); i++) {
            double dist = Distance.calculateDistance(P.get(i).parseVector(), c.parseVector(), "standard");
            if (dist >= max && P.get(i).equals(p) == false) {
                p = P.get(i);
                max = dist;
                argmax = i;
            }
        }
        return new Tuple2(argmax, p);
    }

    public static Tuple2<Integer, Point> mostFar(ArrayList<Point> P, ArrayList<Point> S) {
        Point p = null;
        double max = Double.MIN_VALUE;
        int argmax = -1;
        for (int i = 0; i < S.size(); i++) {
            Tuple2<Integer, Point> t = Utility.mostClose(P, S.get(i));
            double dist = Distance.calculateDistance(t._2.parseVector(), S.get(i).parseVector(), "standard");
            if (dist >= max && P.get(i).equals(p) == false) {
                p = S.get(i);
                max = dist;
                argmax = i;
            }
        }
        return new Tuple2(argmax, p);
    }

    public static double[] toDoubleArray(ArrayList v) {
        // Una fantastica porcata
        double[] dd = new double[v.size()];
        for (int i = 0; i < v.size(); i++) {
            dd[i] = (Double) v.get(i);
        }
        return dd;
    }
    
    
  public static RDD<Vector> PCA( List<Vector> rowsList,SparkContext sc) 
  {     
    JavaRDD<Vector> rows = JavaSparkContext.fromSparkContext(sc).parallelize(rowsList);
    // Create a RowMatrix from JavaRDD<Vector>.
    RowMatrix mat = new RowMatrix(rows.rdd());
    // Compute the top 3 principal components.
    Matrix pc = mat.computePrincipalComponents(3);
    RowMatrix projected = mat.multiply(pc);
    RDD<Vector> rows1 = projected.rows();
    return rows1;
  }
  
  static JavaDoubleRDD removeOutliersMR(JavaDoubleRDD rdd) 
  {
    final StatCounter summaryStats = rdd.stats();
    final Double stddev = Math.sqrt(summaryStats.variance());
    return rdd.filter(new Function<Double, Boolean>() { public Boolean call(Double x) {
          return (Math.abs(x - summaryStats.mean()) < 3 * stddev);
        }});
  }
    
    
}
