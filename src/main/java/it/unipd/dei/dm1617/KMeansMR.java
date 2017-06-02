/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import static it.unipd.dei.dm1617.KMeans.contMeans;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import scala.Tuple4;

/**
 *
 * @author DavideDP
 */
public class KMeansMR 
{
    public static void main(String[] args) throws Exception {
        primoTestMR(args);        
    }
    public static void secondoTestMR(String[] args) 
    {
        String nomeMetodo1="Normale ";
        String nomeMetodo2="MR";
        ArrayList<Point> P = Tester.caricoSpark("Confronto tra "+nomeMetodo1+" e "+nomeMetodo2,"A3.txt");  
        int k=10;
        int numIterations=10;
        Tuple4<Double, Double, Double, Double> test = 
        Tester.testIteratoConfronto
        (
                 P,k,numIterations,
                (P2,k2)->{return CentersBuilder.initMedianCenters(P2, k2)  ;},//ClusteringBuilder.kmeansPlusPlus(P2, k2)  ClusteringBuilder.getRandomCenters(P2, k2)
                //Corrisponde a P,S,k da passare all'algoritmo da testare specifico
                (t)->{return KMeans.kmeansAlgorithm(t._1(), t._2(), t._3());},
                (t)->{return KMeansMR.kmeansAlgorithmMR(t._1(), t._2(), t._3());},
                (C)->{return C.kmeans();}
        );
        //STAMPO INFO
        System.out.println("Tempo "+nomeMetodo1+": "+test._1());
        System.out.println("Tempo "+nomeMetodo2+": "+test._3());
        System.out.println("Score "+nomeMetodo1+": "+test._2());
        System.out.println("Score "+nomeMetodo2+": "+test._4());    
        
        System.out.println("#Iterazioni "+nomeMetodo1+": "+KMeans.contMeans/(300));
        System.out.println("#Iterazioni "+nomeMetodo2+": "+KMeans.contEuristico/(300));
        
        double ratioScore=(Math.abs(test._2() -test._4())/(test._4()))*100;
        System.out.println("ImprovementRatioScore: "+ratioScore+"%");
         double ratioTime=(Math.abs(test._1() -test._3())  /  Math.max(test._1(),test._3())    )*100;
        System.out.println("ImprovementRatioTime: "+ratioTime+"%");        
    }   
    
    public static void primoTestMR(String[] args) 
    {
        System.out.println("Test Primo MR");
        ArrayList<Point> P = Tester.caricoSpark("Test MR","A3.txt");  
        System.out.println("Caricato");
        int k=10;   
        ArrayList<Point> S=CentersBuilder.getRandomCenters(P, k);
        double start=System.currentTimeMillis();
        Clustering C1 = KMeans.kmeansAlgorithm(P, S, k);
        double end =System.currentTimeMillis();
        System.out.println("TimeNormale:"+(end-start));
        System.out.println("C1: "+C1.kmeans());
        P=Utility.copy(P);
        start=System.currentTimeMillis();
        Clustering C2 =KMeansMR.kmeansAlgorithmMR(P, S, k);
        end =System.currentTimeMillis();
        System.out.println("Time MR:"+(end-start));
        //Clustering C2 =KMeansMR.PartitionMR_optimized(P, C1.getCenters(), k, true, false, null);
        System.out.println("C2: "+C2.kmeans());      
        
        start=System.currentTimeMillis();
        C1.kmeans();
        end =System.currentTimeMillis();
        System.out.println("Time funz.obj.locale:"+(end-start));
        start=System.currentTimeMillis();
        C2.kmeansMR(JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()));
        end =System.currentTimeMillis();
        System.out.println("Time funz.obj MR:"+(end-start));        
        while(true);
    }
    
    public static void testXMeans(String[] args) throws IOException, Exception 
    {
        String nomeMetodo2="XMeans";
        ArrayList<Point> P = Tester.caricoSpark(nomeMetodo2,"A3.txt");  
        int k=5;
        Clustering bestX = XMeans.XMeans(P, CentersBuilder.kmeansPlusPlus(P, k), k, 100);
        System.out.println("KTrovato: "+bestX.getK());
    }

    public static Clustering kmeansAlgorithmMR(ArrayList<Point> P, ArrayList<Point> S, int k) 
        {
        Clustering primeClustering = KMeansMR.PartitionMR_optimized(P, S, k, true, false, null);
        boolean stopping_condition = false;
        double phi = primeClustering.kmeans();
        boolean stoppingCondition = false;
        int cont = 0;
        while (!stoppingCondition) 
        {
            //Calcolo nuovo Clustering
            ArrayList<Point> centroids = primeClustering.getCentroids();
            //Clustering secondClustering = ClusteringBuilder.Partition(P, centroids, k);
            Clustering secondClustering = KMeansMR.PartitionMR_optimized(P, centroids, k, false, false, primeClustering);
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
        contMeans = contMeans + cont;
        return primeClustering;
    }

    public static Clustering PartitionMR_optimized(ArrayList<Point> P, ArrayList<Point> S, int k, boolean first, boolean euristic, Clustering C2) {
        if (first) {
            return PartitionMR_optimized_first(P,S,k);

        } else 
        {
            if(euristic)
            {
                return PartitionMR_optimized_euristic(P,S,k,C2);
            }
            else
            {
                return PartitionMR_optimized_Noneuristic(P,S,k,C2);
            }
        }
    }
    
    public static Clustering PartitionMR_optimized_euristic(ArrayList<Point> P, ArrayList<Point> S, int k, Clustering C2) {
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
        for (Point p : P) {
            p.setID_Cluster(C2.getMap().get(p).getID());//Aggiorno i Cluster di ogni Punto
        }
        JavaRDD<Point> points = sc.parallelize(P);//Parallelizzo
        HashMap<String, Point> mapCenters = new HashMap<String, Point>();//ID Cluster -> Centro
        for (int i = 0; i < k; i++) {
            mapCenters.put(C2.getClusters().get(i).getID(), S.get(i));
        }
        Broadcast<HashMap<String, Point>> broadcast = sc.broadcast(mapCenters);
        HashMap<String, Cluster> mapSpeed = new HashMap<String, Cluster>();//ID Cluster -> Cluster
        for (int i = 0; i < k; i++) {
            mapSpeed.put(C2.getClusters().get(i).getID(), C2.getClusters().get(i));
        }
        JavaRDD<Tuple2<Point, String>> map2 = points.map((p)
                -> {
            HashMap<String, Point> mapCenters2 = broadcast.getValue();
            Point center = mapCenters2.get(p.getID_Cluster());
            double dist2 = Distance.calculateDistance(p.parseVector(), center.parseVector());
            if(p.getDist()>dist2)
            {
                p.setDist(dist2);
                return new Tuple2(p,p.getID_Cluster());
            }
            Iterator it = mapCenters2.entrySet().iterator();
            ArrayList<String> ID_Clusters = new ArrayList<String>();
            ArrayList<Point> centersOfClusters = new ArrayList<Point>();
            
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                ID_Clusters.add((String) pair.getKey());
                centersOfClusters.add((Point) pair.getValue());
            }
            Tuple2<Integer, Point> mostClose = Utility.mostClose(centersOfClusters, p);
            p.setDist(Distance.calculateDistance(p.parseVector(), mostClose._2.parseVector()));
            String ID_Cluster = ID_Clusters.get(mostClose._1);
            return new Tuple2(p, ID_Cluster);

        }
        );
        List<Tuple2<Point, String>> collect = map2.collect();
        for (Tuple2<Point, String> t : collect) {
            C2.assignPoint(t._1, mapSpeed.get(t._2));
        }
        C2.setCenters(S);
        return C2;

    }     
 
 
 
    public static Clustering PartitionMR_optimized_Noneuristic(ArrayList<Point> P, ArrayList<Point> S, int k, Clustering C2) {
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
        for (Point p : P) {
            p.setID_Cluster(C2.getMap().get(p).getID());//Aggiorno i Cluster di ogni Punto
        }
        JavaRDD<Point> points = sc.parallelize(P);//Parallelizzo
        HashMap<String, Point> mapCenters = new HashMap<String, Point>();//ID Cluster -> Centro
        for (int i = 0; i < k; i++) {
            mapCenters.put(C2.getClusters().get(i).getID(), S.get(i));
        }
        Broadcast<HashMap<String, Point>> broadcast = sc.broadcast(mapCenters);
        HashMap<String, Cluster> mapSpeed = new HashMap<String, Cluster>();//ID Cluster -> Cluster
        for (int i = 0; i < k; i++) {
            mapSpeed.put(C2.getClusters().get(i).getID(), C2.getClusters().get(i));
        }
        JavaRDD<Tuple2<Point, String>> map2 = points.map((p)
                -> {
            HashMap<String, Point> mapCenters2 = broadcast.getValue();
            Point center = mapCenters2.get(p.getID_Cluster());
            double dist2 = Distance.calculateDistance(p.parseVector(), center.parseVector());
            Iterator it = mapCenters2.entrySet().iterator();
            ArrayList<String> ID_Clusters = new ArrayList<String>();
            ArrayList<Point> centersOfClusters = new ArrayList<Point>();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                ID_Clusters.add((String) pair.getKey());
                centersOfClusters.add((Point) pair.getValue());
            }
            Tuple2<Integer, Point> mostClose = Utility.mostClose(centersOfClusters, p);
            p.setDist(Distance.calculateDistance(p.parseVector(), mostClose._2.parseVector()));
            String ID_Cluster = ID_Clusters.get(mostClose._1);
            return new Tuple2(p, ID_Cluster);

        }
        );
        List<Tuple2<Point, String>> collect = map2.collect();
        for (Tuple2<Point, String> t : collect) {
            C2.assignPoint(t._1, mapSpeed.get(t._2));
        }
        C2.setCenters(S);
        return C2;

    }
            
        

   
 public static Clustering PartitionMR_optimized_first(ArrayList<Point> P, ArrayList<Point> S, int k)
 {
             JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
            JavaRDD<Point> points = sc.parallelize(P);
            ArrayList<Cluster> clusters = new ArrayList<Cluster>();
            HashMap<Point, Cluster> map = new HashMap<Point, Cluster>();
            for (int i = 0; i < k; i++) {
                Cluster C = new Cluster();
                C.setCenter(S.get(i));
                clusters.add(C);
            }
            //for (Point p:P) {map.put(p, null);}           
            //ID Centro-> Cluster
            HashMap<String, Cluster> mapCenters = new HashMap<String, Cluster>();
            for (int i = 0; i < k; i++) {
                mapCenters.put(clusters.get(i).getCenter().getID(), clusters.get(i));
            }
            //Faccio broadcast dei centri
            Broadcast<ArrayList<Point>> broadcast = sc.broadcast(S);
            //Per ogni punto gli salvo l'ID del Centro
            //Dopo sapendo id del centro riesco a trovari il riferimento al cluster
            JavaRDD<Tuple2<Point, String>> map2 = points.map((p)
                    -> {
                Point mostClose = Utility.mostClose(broadcast.value(), p)._2;
                String ID_Centro = mostClose.getID();
                return new Tuple2(p, ID_Centro);
            }
            );
            //meglio ancora sarebbe avere solo gli ID dei punti
            //AVENDO COPPIE ID PUNTO->ID CENTRO DEL CLUSTER ORA POSSO CREARMI IL CLUSTER IN LOCALE
            //questa parte è uguale a quello usata nel metodo partition di ClusteringBuilder
            List<Tuple2<Point, String>> collect = map2.collect();
            for (Tuple2<Point, String> t : collect) {
                Cluster cluster = mapCenters.get(t._2);
                Point p = t._1;
                map.put(p, cluster);
                cluster.getPoints().add(p);
            }
            return new Clustering(P, clusters, S, map);
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
