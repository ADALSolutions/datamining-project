/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

/**
 *
 * @author DavideDP
 */
public class ClusteringBuilderMR {

    //kmeans con partition buono
    //p.s. nello stesso modo possiamo calcolare in parallelo valori phi di kmeans,kcenter,ecc.
    public static Clustering kmeansAlgorithmMR_withPartitionMR_optimized(JavaSparkContext sc, ArrayList<Point> points, ArrayList<Point> SList, int k) {
        int numPartitions = sc.defaultParallelism();
        JavaRDD<Point> P = sc.parallelize(points, numPartitions);
        Broadcast<ArrayList<Point>> S = sc.broadcast(SList);
        P.cache();
        Clustering primeClustering = ClusteringBuilderMR.PartitionMR_optimezed(points, P, S, k);
        boolean stopping_condition = false;
        double phi = primeClustering.kmeansMR(sc);
        boolean stoppingCondition = false;

        while (!stoppingCondition) {
            //Calcolo nuovo Clustering
            ArrayList<Point> centroids = ClusteringBuilderMR.calculateCentroidsMR(sc, primeClustering);
            Broadcast<ArrayList<Point>> Snew = sc.broadcast(centroids);
            Clustering secondClustering = ClusteringBuilderMR.PartitionMR_optimezed(points, P, Snew, k);
            double phikmeans = secondClustering.kmeansMR(sc);//questo si può fare in parellelo

            //Valuto se quello nuovo è megliore rispetto a quello vecchio
            if (phi > phikmeans) {
                phi = phikmeans;
                primeClustering = secondClustering;
            } else {
                stoppingCondition = true;
            }
        }
        return primeClustering;
    }

    //usa partition complicato
    public static Clustering kmeansAlgorithmMR_withPartitionMR(JavaSparkContext sc, ArrayList<Point> points, ArrayList<Point> SList, int k) {
        int numPartitions = sc.defaultParallelism();
        JavaRDD<Point> P = sc.parallelize(points, numPartitions);
        Broadcast<ArrayList<Point>> S = sc.broadcast(SList);
        P.cache();
        ArrayList<Cluster> PartitionMR = ClusteringBuilderMR.PartitionMR_old(P, S, k);
        Clustering primeClustering = new Clustering(points, PartitionMR, SList);
        boolean stopping_condition = false;
        double phi = primeClustering.kmeans();
        boolean stoppingCondition = false;

        while (!stoppingCondition) {
            //Calcolo nuovo Clustering
            ArrayList<Point> centroids = ClusteringBuilderMR.calculateCentroidsMR(sc, primeClustering);
            Broadcast<ArrayList<Point>> Snew = sc.broadcast(centroids);
            ArrayList<Cluster> PartitionMR1 = ClusteringBuilderMR.PartitionMR_old(P, Snew, k);
            Clustering secondClustering = new Clustering(points, PartitionMR1, Snew.value());
            double phikmeans = secondClustering.kmeans();
            //Valuto se quello nuovo è megliore rispetto a quello vecchio
            if (phi > phikmeans) {
                phi = phikmeans;
                primeClustering = secondClustering;
            } else {
                stoppingCondition = true;
            }
        }
        return primeClustering;
    }

    //quello buono,semplice e veloce
    public static Clustering PartitionMR_optimezed(ArrayList<Point> P, JavaRDD<Point> points, Broadcast<ArrayList<Point>> Sbroadcast, int k) {
        ArrayList<Point> S = Sbroadcast.value();
        //creo tuple del tipo <Punto p,centro assegnato>
        //parte in cui serve il lavoro in parralelo
        //per ogni punto calcolo la distanza dai centri e scelgo quello più vicino usando il metodo di Utility

        JavaRDD<Tuple2<Point, Point>> map2 = points.map((p)
                -> {
            Point mostClose = Utility.mostClose(S, p)._2;
            return new Tuple2(p, mostClose);
        }
        );
        //meglio ancora sarebbe avere solo gli ID dei punti

        //AVENDO COPPIE PUNTO->CENTRO DEL CLUSTER ORA POSSO CREARMI IL CLUSTER IN LOCALE
        //questa parte è uguale a quello usata nel metodo partition di ClusteringBuilder
        List<Tuple2<Point, Point>> collect = map2.collect();
        ArrayList<Cluster> clusters = new ArrayList<Cluster>();
        HashMap<Point, Cluster> map = new HashMap<Point, Cluster>();
        // for i <- 1 to k do C_i <- null
        for (int i = 0; i < k; i++) {
            Cluster C = new Cluster();
            C.setCenter(S.get(i));
            clusters.add(C);
        }

        for (Tuple2<Point, Point> t : collect) {
            Point p = t._1;
            for (Cluster C : clusters) {
                if (C.getCenter().equals(t._2)) {
                    map.put(p, C);
                    C.getPoints().add(p);
                }
            }

        }
        return new Clustering(P, clusters, S);
    }

    //molto più complicato dell'altro
    public static ArrayList<Cluster> PartitionMR_old(JavaRDD<Point> points, Broadcast<ArrayList<Point>> Sbroadcast, int k) {
        //Broadcast<ArrayList<Point>> broadcast = sc.broadcast(S);
        long count = points.count();
        int size = (int) (((double) count) / Math.sqrt(count));
        //RAGRUPPO I PUNTI PER CHIAVE%SIZE
        JavaPairRDD<Integer, Iterable<Point>> groups = points.groupBy((p) -> {
            return Integer.parseInt(p.getID()) % size;
        });
        //PER OGNI GRUPPO DI PUNTI FACCIO IL CLUSTERING CON PARTITION
        JavaRDD<Clustering> map = groups.map((Tuple2<Integer, Iterable<Point>> tupla) -> {
            Iterator<Point> it = tupla._2.iterator();
            ArrayList<Point> punti = new ArrayList<Point>();
            while (it.hasNext()) {
                punti.add(it.next());
            }
            return ClusteringBuilder.Partition(punti, Sbroadcast.getValue(), k);
        });
        //DIVIDO CLUSTERING IN CLUSTERS
        JavaRDD<Cluster> flatMap = map.flatMap((Clustering C) -> {

            int sum = 0;
            //STAMPO PER OGNI CLUSTERING LE INFORMAZIONI PER OGNI CLUSTER
            System.out.println("Numero di punti: " + C.getPoints().size());
            for (int i = 0; i < C.getK(); i++) {
                System.out.println(C.getClusters().get(i).toString() + " : " + C.getClusters().get(i).size());
                sum += C.getClusters().get(i).size();
            }
            System.out.println("-----------------------------------------");

            return C.getClusters().iterator();
        });
        //RAGRUPPO IN BASE AL CENTRO
        JavaPairRDD<Integer, Iterable<Cluster>> groupBy = flatMap.groupBy((C) -> {
            String ID = C.getID();
            int length = ID.length();

            Integer s = Integer.parseInt(ID.substring(7, length));
            //System.out.println(s);
            return s % k;
        });
        //UNISCO CLUSTER CON STESSO CENTRO
        JavaRDD<Cluster> map1 = groupBy.map((Tupla) -> {
            Iterator<Cluster> it = Tupla._2.iterator();
            Cluster union = new Cluster();
            int cont = 0;
            System.out.println("Ragruppamento");
            Point center = null;
            while (it.hasNext()) {
                cont++;
                Cluster cl = it.next();
                center = cl.getCenter();
                System.out.println(cont + " : " + cl + "  size:" + cl.size());
                union = Cluster.union(union, cl);

            }
            union.setCenter(center);
            System.out.println("UNION SIZE: " + union.size());
            System.out.println("-----------------------------------------");
            return union;
        });
        ArrayList<Cluster> clusters = new ArrayList<Cluster>(map1.collect());
        return clusters;
    }

    public static ArrayList<Point> calculateCentroidsMR(JavaSparkContext sc, Clustering CL) {
        ArrayList<Point> al = new ArrayList<Point>();
        for (Cluster C : CL.getClusters()) {
            JavaRDD<Point> parallelize = sc.parallelize(C.getPoints());
            Broadcast<Point> b = sc.broadcast(C.getCenter());
            Vector centroid = ClusteringBuilderMR.calculateCentroidMR(parallelize, b, 0);
            PointSpark p = new PointSpark(centroid);
            al.add(p);
        }
        return al;
    }

    public static Vector calculateCentroidMR(JavaRDD<Point> points, Broadcast<Point> b, int dim) {
        Vector center = b.value().parseVector();
        Vector sum = Vectors.zeros(dim);
        JavaRDD<Vector> map = points.map((p) -> {
            return p.parseVector();
        });
        Vector reduce = map.reduce((v1, v2) -> {
            BLAS.axpy(1, v1, v2);
            return v2;
        });
        return reduce;
    }

}
