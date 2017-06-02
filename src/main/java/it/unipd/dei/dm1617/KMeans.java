/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import java.util.ArrayList;
import java.util.HashMap;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

/**
 *
 * @author DavideDP
 */
public class KMeans 
{

    public static int contEuristico;
    public static int contMeans;

    public static Clustering Partition_optimized(ArrayList<Point> P, ArrayList<Point> S, int k, boolean first, boolean euristic, Clustering C2) {
        if (first) {
            if (S.size() == k) {
            } else {
                throw new IllegalArgumentException("S must have size k ");
            }
            long start = System.currentTimeMillis();
            ArrayList<Cluster> clusters = new ArrayList<Cluster>();
            HashMap<Point, Cluster> map = new HashMap<Point, Cluster>();
            for (int i = 0; i < k; i++) {
                Cluster C = new Cluster();
                C.setCenter(S.get(i));
                clusters.add(C);
            }
            if (k == 1) {
                clusters.get(0).getPoints().addAll(P);
                return new Clustering(P, clusters, S);
            }
            for (Point p : P) {
                Vector parseVector = p.parseVector();
                Tuple2<Integer, Point> mostClose = Utility.mostClose(S, p);
                Cluster C = clusters.get(mostClose._1);
                map.put(p, C);
                C.getPoints().add(p);
                p.setDist(Distance.calculateDistance(p.parseVector(), C.getCenter().parseVector()));
            }
            long end = System.currentTimeMillis();
            //System.out.println(","+(end-start));
            //Tempo Partition Inizial
            return new Clustering(P, clusters, S); //S centroidi
        } else {
            //Aggiornamento Clustering
            int cont2 = 0;
            for (Point p : P) {
                Vector parseVector = p.parseVector();
                Cluster C = C2.getMap().get(p);
                int index = C2.getClusters().indexOf(C);
                Point center = S.get(index);
                double dist2 = Distance.calculateDistance(parseVector, center.parseVector());
                if (euristic) {
                    if (p.getDist() > dist2) {
                        //long start = System.nanoTime();
                        cont2++;
                        //System.out.println("ENTRO");
                        p.setDist(dist2);
                        //long end = System.nanoTime();
                        //System.out.println("PezzoCorto: "+(end-start));
                    } else {
                        //long start = System.nanoTime();
                        ArrayList<Cluster> clusters = C2.getClusters();
                        //ArrayList<Point> centers = C2.getCenters();
                        Tuple2<Integer, Point> mostClose = Utility.mostClose(S, p);
                        C = clusters.get(mostClose._1);
                        C2.assignPoint(p, C);
                        p.setDist(Distance.calculateDistance(p.parseVector(), mostClose._2.parseVector()));
                        //long end = System.nanoTime();
                        //System.out.println("PezzoLungo: "+(end-start));
                    }
                } else {
                    ArrayList<Cluster> clusters = C2.getClusters();
                    //ArrayList<Point> centers = C2.getCenters();
                    Tuple2<Integer, Point> mostClose = Utility.mostClose(S, p);
                    C = clusters.get(mostClose._1);
                    C2.assignPoint(p, C);
                    p.setDist(Distance.calculateDistance(p.parseVector(), mostClose._2.parseVector()));
                }
            }
            //System.out.println("Risparmio : "+cont2);
            for (int i = 0; i < S.size(); i++) {
                C2.getClusters().get(i).setCenter(S.get(i));
            }
            return C2;
        }
    }

    public static Clustering kmeansAlgorithm(ArrayList<Point> P, ArrayList<Point> S, int k) {
        //Clustering primeClustering = ClusteringBuilder.Partition(P, S, k);
        Clustering primeClustering = KMeans.Partition_optimized(P, S, k, true, false, null);
        boolean stopping_condition = false;
        double phi = primeClustering.kmeans();
        boolean stoppingCondition = false;
        int cont = 0;
        while (!stoppingCondition) {
            //Calcolo nuovo Clustering
            ArrayList<Point> centroids = primeClustering.getCentroids();
            //Clustering secondClustering = ClusteringBuilder.Partition(P, centroids, k);
            Clustering secondClustering = KMeans.Partition_optimized(P, centroids, k, false, false, primeClustering);
            double phikmeans = secondClustering.kmeans();
            //Valuto se quello nuovo è megliore rispetto a quello vecchio
            if (phi > phikmeans) {
                phi = phikmeans;
                primeClustering = secondClustering;
            } else {
                stoppingCondition = true;
            }
            cont++;
        }
        //System.out.println("Cont KMeans: " + cont);
        //ClusteringBuilder.numIter = cont;
        contMeans = contMeans + cont;
        return primeClustering;
    }

    public static Clustering kmeansEuristic(ArrayList<Point> P, ArrayList<Point> S, int k) {
        //System.out.println("Inizio algoritmo");
        Clustering primeClustering = KMeans.Partition_optimized(P, S, k, true, true, null);
        boolean stopping_condition = false;
        double phi = primeClustering.kmeans();
        boolean stoppingCondition = false;
        int cont = 0;
        while (!stoppingCondition) {
            cont++;
            //Calcolo nuovo Clustering
            ArrayList<Point> centroids = primeClustering.getCentroids();
            Clustering secondClustering = KMeans.Partition_optimized(P, centroids, k, false, true, primeClustering);
            double phikmeans = secondClustering.kmeans();
            //Valuto se quello nuovo è megliore rispetto a quello vecchio
            //System.out.println(""+Math.abs(phi-phikmeans)+"     "+1/(double)cont);
            if (phi > phikmeans) {
                phi = phikmeans;
                primeClustering = secondClustering;
            } else {
                stoppingCondition = true;
            }
            /*if (cont >= ClusteringBuilder.numIter / 2) {
            break;
            }*/
        }
        //System.out.println("Cont Euristico: " + cont);
        contEuristico = contEuristico + cont;
        return primeClustering;
    }
    
}
