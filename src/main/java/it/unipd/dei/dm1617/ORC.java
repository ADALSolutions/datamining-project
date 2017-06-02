/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;

/**
 *
 * @author DavideDP
 */
public class ORC {

    public static Clustering ORC_old(int numIterations, double threshold, ArrayList<Point> P) throws IOException {
        Clustering C = XMeans.XMeans(P, CentersBuilder.kmeansPlusPlus(P, 2), 2, 20);
        for (int i = 0; i < numIterations; i++) {
            System.out.println("Entro For");
            ArrayList<Point> points = C.getPoints();
            double dmax = Collections.max(points).getDist();
            Tuple2<Double, Double> t = Utility.varianceMedia(points);
            double std = Math.sqrt(t._2);
            double media = t._1;
            ArrayList<Point> remove = new ArrayList<Point>();
            System.out.println("dmax: " + dmax);
            System.out.println("variance: " + std);
            for (int j = 0; j < points.size(); j++) {
                System.out.println("ratio: " + points.get(j).getDist() / dmax);
                if ((points.get(j).getDist() - media) > (threshold * std)) {
                    System.out.println("rimuovo");
                    remove.add(points.get(j));
                }
            }
            P.removeAll(remove);
            C = KMeans.kmeansAlgorithm(P, C.getCentroids(), C.getK());
        }
        return C;
    }

    public static Clustering ORC(int numIterations, double threshold, ArrayList<Point> P) throws IOException {
        int init = P.size();
        ArrayList<Point> removeAll = new ArrayList<Point>();
        Clustering C = XMeans.XMeans(P, CentersBuilder.kmeansPlusPlus(P, 2), 2, 20);
        for (int i = 0; i < numIterations; i++) {
            System.out.println("Entro For");
            for (Cluster CL : C.getClusters()) {
                ArrayList<Point> points = CL.getPoints();
                //double dmax=Collections.max(points).getDist();
                Tuple2<Double, Double> t = Utility.varianceMedia(points);
                double std = Math.sqrt(t._2);
                double media = t._1;
                ArrayList<Point> remove = new ArrayList<Point>();
                //System.out.println("dmax: "+dmax);
                //System.out.println("variance: "+std);
                for (int j = 0; j < points.size(); j++) {
                    //System.out.println(("ratio: "+points.get(j).getDist() /dmax));
                    if ((points.get(j).getDist() - media) > (threshold * std)) {
                        remove.add(points.get(j));
                    }
                }
                //System.out.println("#rimozioni: "+remove.size());
                removeAll.addAll(remove);
                for (Point p : remove) {
                    C.removePoint(p);
                }
                C = KMeans.kmeansAlgorithm(P, C.getCentroids(), C.getK());
            }
        }
        System.out.println("Diff: " + (init - C.getPoints().size()));
        Clustering output = KMeans.kmeansAlgorithm(removeAll, CentersBuilder.kmeansPlusPlus(removeAll, 1), 1);
        Utility.writeOuptut("output.txt", output);
        return C;
    }

    static JavaDoubleRDD removeOutliersMR(JavaDoubleRDD rdd) {
        final StatCounter summaryStats = rdd.stats();
        final Double stddev = Math.sqrt(summaryStats.variance());
        return rdd.filter(new Function<Double, Boolean>() {
            public Boolean call(Double x) {
                return Math.abs(x - summaryStats.mean()) < 3 * stddev;
            }
        });
    }
    
}
