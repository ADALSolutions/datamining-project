/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

/**
 *
 * @author DavideDP
 */
public class ClusteringBuilderMR 
{

    public static ArrayList<Cluster> PARTITION(JavaRDD<Point> points, Broadcast<ArrayList<Point>> Sbroadcast, int k) {
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
            System.out.println("Numero di punti: "+C.getPoints().size());
            for (int i = 0; i < C.getK(); i++) {
                System.out.println(C.getClusters().get(i).toString() + " : " + C.getClusters().get(i).size());
                sum += C.getClusters().get(i).size();
            }
            System.out.println("-----------------------------------------");

            return C.getClusters().iterator();
        });
        //RAGRUPPO IN BASE AL CENTRO
        JavaPairRDD<Integer, Iterable<Cluster>> groupBy = flatMap.groupBy((C) -> {
            String ID=C.getID();
            int length=ID.length();
           
            Integer s=Integer.parseInt(ID.substring(7,length));
            //System.out.println(s);
            return s%k;
        });
        //UNISCO CLUSTER CON STESSO CENTRO
        JavaRDD<Cluster> map1 = groupBy.map((Tupla) -> {
            Iterator<Cluster> it = Tupla._2.iterator();
            Cluster union = new Cluster();
            int cont=0;
            System.out.println("Ragruppamento");
            Point center=null;
            while (it.hasNext()) {
                cont++;
                Cluster cl=it.next();
                center=cl.getCenter();
                System.out.println(cont+" : "+cl+"  size:"+cl.size());                
                union=Cluster.union(union,cl );
                
            }
            union.setCenter(center);
            System.out.println("UNION SIZE: "+union.size());
            System.out.println("-----------------------------------------");            
            return union;
        });
        ArrayList<Cluster> clusters = new ArrayList<Cluster>( map1.collect());
        return clusters;   

    }
    public static Clustering PARTITION_old(ArrayList<Point> P, ArrayList<Point> S, int k) {
        SparkConf sparkConf = new SparkConf(true).setAppName("Compute primes");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        ArrayList<ArrayList<Point>> AL = new ArrayList<ArrayList<Point>>();
        int size = (int) (P.size() / Math.sqrt(P.size()));
        int cont = 0;
        ArrayList<Point> alp = new ArrayList<Point>();
        for (int i = 0; i < P.size(); i++) {
            alp.add(P.get(i));
            cont++;
            if (cont >= size) {
                AL.add(alp);
                alp = new ArrayList<Point>();
            }
        }
        //groupBy
        JavaRDD< ArrayList<Point>> points = sc.parallelize(AL);
        Clustering C = null;

        Broadcast<ArrayList<Point>> broadcast = sc.broadcast(S);
        JavaRDD<Clustering> map = points.map((Pgrande) -> {
            return ClusteringBuilder.Partition(Pgrande, broadcast.getValue(), k);
        });
        JavaRDD<Cluster> flatMap = map.flatMap((R) -> {
            //String[] split = "Word".split(" ");
            Cluster[] split = new Cluster[C.getK()];
            return Stream.of(R.getClusters().toArray(split)).iterator();
        });
        JavaPairRDD<Object, Iterable<Cluster>> groupBy = flatMap.groupBy(new Function<Cluster, Object>() {
            @Override
            public Point call(Cluster R) throws Exception {
                return R.getCenter();
            }
        });
        JavaRDD<Cluster> map1 = groupBy.map(new Function<Tuple2<Object, Iterable<Cluster>>, Cluster>() {
            @Override
            public Cluster call(Tuple2<Object, Iterable<Cluster>> t1) throws Exception {
                Cluster union = new Cluster();
                Iterator<Cluster> iterator = t1._2.iterator();
                while (iterator.hasNext()) {
                    Cluster next = iterator.next();
                    union = Cluster.union(union, next);

                }
                return union;
            }

        });
        Iterator<Cluster> iterator = map1.collect().iterator();
        ArrayList<Cluster> ALC = new ArrayList<Cluster>();
        while (iterator.hasNext()) {
            ALC.add(iterator.next());
        }

        return new Clustering(P, ALC, S);
    }

}
