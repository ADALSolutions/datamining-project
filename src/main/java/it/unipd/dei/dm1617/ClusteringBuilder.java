/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import java.util.ArrayList;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.json4s.Merge;
import scala.Tuple2;

/**
 *
 * @author DavideDP
 */
public class ClusteringBuilder implements Serializable {

    public static int numIter = -1;

    public static Clustering Partition(ArrayList<Point> P, ArrayList<Point> S, int k) {
        if ((S.size() == k)) {
        } else {
            throw new IllegalArgumentException("S must have size k ");
        }

        ArrayList<Cluster> clusters = new ArrayList<Cluster>();
        HashMap<Point, Cluster> map = new HashMap<Point, Cluster>();
        // for i <- 1 to k do C_i <- null
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
        return new Clustering(P, clusters, S);//S centroidi        
    }

    public static Clustering FarthestFirstTraversal(ArrayList<Point> P, int k) {

        /* rimuovo i punti dal clustering in modo da tener traccia dei rimanenti P\cc
         poi li riaggiungo tutti alla fine, credo sia più efficiente*/
        Point first = P.get(new Random().nextInt(P.size()));
        ArrayList<Point> S = new ArrayList<Point>();
        S.add(first);
        P.remove(first);
        //se p.size<k allora raise error 
        for (int i = 0; i <= k - 2; i++) {
            //prendo come inizio sempre la distanza tra il primo centroide(first) e il primo punto(P.get(0))
            double max = Distance.calculateDistance(first.parseVector(), P.get(0).parseVector());
            int argmax = 0;
            for (int j = 0; j < P.size(); j++) {
                Point esamina = P.get(j);
                for (int l = 0; l < S.size(); l++) {
                    double dist = Distance.calculateDistance(esamina.parseVector(), S.get(l).parseVector());
                    if (dist > max) {
                        max = dist;
                        argmax = j;
                    }
                }
            }
            S.add(P.get(argmax));
            P.remove(argmax);
        }
        P.addAll(S);
        return ClusteringBuilder.Partition(P, S, k);
    }

    public static Clustering kmeansAlgorithm_old(ArrayList<Point> P, ArrayList<Point> S, int k) {
        Clustering primeClustering = KMeans.Partition_optimized(P, S, k, true, false, null);
        boolean stopping_condition = false;
        double phi = primeClustering.kmeans();
        boolean stoppingCondition = false;
        int cont = 0;
        while (!stoppingCondition) {
            //Calcolo nuovo Clustering
            ArrayList<Point> centroids = primeClustering.getCentroids();
            Clustering secondClustering = KMeans.Partition_optimized(P, centroids, k, true, false, primeClustering);
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
//System.out.println("Counting KMeans: " + cont);
        return primeClustering;
    }

    public static Clustering kmeansEuristic_old(ArrayList<Point> P, ArrayList<Point> S, int k) {
        Clustering primeClustering = ClusteringBuilder.PartitionEuristic_old(P, S, k, true, null);;
        boolean stopping_condition = false;
        double phi = primeClustering.kmeans();
        boolean stoppingCondition = false;
        int cont = 0;
        while (!stoppingCondition) {
            cont++;
            //Calcolo nuovo Clustering
            ArrayList<Point> centroids = primeClustering.getCentroids();
            Clustering secondClustering = ClusteringBuilder.PartitionEuristic_old(P, centroids, k, false, primeClustering);
            double phikmeans = secondClustering.kmeans();
            //Valuto se quello nuovo è megliore rispetto a quello vecchio
            if (phi > phikmeans) {
                phi = phikmeans;
                primeClustering = secondClustering;
            } else {
                stoppingCondition = true;
            }
        }
        System.out.println("Cont: " + cont);
        return primeClustering;
    }


    public static Clustering kmeansPlusPlusAlgorithm(ArrayList<Point> P, int k) {
        ArrayList<Point> S = CentersBuilder.kmeansPlusPlus(P, k);
        return KMeans.kmeansAlgorithm(P, S, k);
    }

    public static Clustering PartitioningAroundMedoids(ArrayList<Point> P, ArrayList<Point> S, int k) {
        Clustering C_init = ClusteringBuilder.Partition(P, S, k);
        boolean stopping_condition = false;
        while (!stopping_condition) {
            stopping_condition = true;
            P.removeAll(S);
            boolean condition_exit = false;
            for (int i = 0; i < P.size(); i++) {
                if (condition_exit) {
                    break;
                }
                Point p = P.get(i);
                for (int j = 0; j < S.size(); j++) {
                    P.removeAll(S);//usare array per fare queste rimozioni è pesantissimo
                    ArrayList<Point> Sprimo = (ArrayList<Point>) S.clone();
                    //aggiungo tutti i punti in P per fare il PARTITION
                    P.addAll(S);//contiene anche c e p
                    Point c = S.get(j);
                    Sprimo.remove(c);
                    Sprimo.add(p);
                    //DA QUI IN POI P HA TUTTI I PUNTI
                    Clustering Cprimo = ClusteringBuilder.Partition(P, Sprimo, k);
                    double phimedianPrimo = Cprimo.objectiveFunction("kmedian");
                    double phimedian = C_init.objectiveFunction("kmedian");
                    if (phimedianPrimo < phimedian) {
                        stopping_condition = false;
                        C_init = Cprimo;
                        condition_exit = true;
                        S = Sprimo;
                        break;
                    }

                }
            }
        }
        return C_init;
    }

    public static Clustering hierarchicalClustering(ArrayList<Point> P, int k, BiFunction<Clustering, Clustering, Boolean> f) {
        Clustering C = ClusteringBuilder.Partition(P, CentersBuilder.getRandomCenters(P, k), k);
        ArrayList<Cluster> al = new ArrayList<Cluster>();
        for (Point p : P) {
            ArrayList<Point> alp = new ArrayList<Point>();
            alp.add(p);
            Cluster cluster = new Cluster(alp);
        }
        MergingCriterion mc = new MergingCriterion(C);
        while (!f.apply(C, null)) {
            mc.mergeSingleLinkage();
        }
        return C;
    }

    public static Clustering clusteringAlgorithm(ArrayList<Point> P, int k, BiFunction<Clustering, Clustering, Boolean> f, String algorithm) {
        if (algorithm.equals("kcenter")) {
            return FarthestFirstTraversal(P, k);
        } else if (algorithm.equals("kmeans")) {
            ArrayList<Point> S = CentersBuilder.getRandomCenters(P, k);
            return KMeans.kmeansAlgorithm(P, S, k);
        } else if (algorithm.equals("kmeansPlusPlus")) {
            return kmeansPlusPlusAlgorithm(P, k);
        } else if (algorithm.equals("kmedian")) {
            ArrayList<Point> S = CentersBuilder.getRandomCenters(P, k);
            return PartitioningAroundMedoids(P, S, k);
        } else if (algorithm.equals("hierarchical") && f != null) {
            return hierarchicalClustering(P, k, f);
        }
        return null;
    }


    public static Clustering PartitionEuristic_old(ArrayList<Point> P, ArrayList<Point> S, int k, boolean first, Clustering C2) {
        if (first) {
            if ((S.size() == k)) {
            } else {
                throw new IllegalArgumentException("S must have size k ");
            }

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
            return new Clustering(P, clusters, S);//S centroidi  
        } else {
            if ((S.size() == k)) {
            } else {
                throw new IllegalArgumentException("S must have size k ");
            }

            ArrayList<Cluster> clusters = new ArrayList<Cluster>();
            HashMap<Point, Cluster> map = new HashMap<Point, Cluster>();
            // for i <- 1 to k do C_i <- null
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
                Point center = C2.getMap().get(p).getCenter();
                int index = C2.getCenters().indexOf(center);
                Point centroid = S.get(index);
                double dist2 = Distance.calculateDistance(parseVector, centroid.parseVector());
                if (p.getDist() == dist2) {//System.out.println("BAKA");
                }
                if (p.getDist() > dist2) {
                    //System.out.println("ENTRO");           
                    p.setDist(dist2);
                    Cluster CL = clusters.get(index);
                    CL.getPoints().add(p);
                    map.put(p, CL);

                } else {
                    Tuple2<Integer, Point> mostClose = Utility.mostClose(S, p);
                    Cluster C = clusters.get(mostClose._1);
                    map.put(p, C);
                    C.getPoints().add(p);
                    p.setDist(Distance.calculateDistance(p.parseVector(), C.getCenter().parseVector()));
                }

            }
            return new Clustering(P, clusters, S);

        }
    }
    
}
