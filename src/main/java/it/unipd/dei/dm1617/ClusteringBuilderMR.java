/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 *
 * @author DavideDP
 */
public class ClusteringBuilderMR 
{
    public static Clustering PARTITION(ArrayList<Point> P, ArrayList<Point> S,int k )
    {
    SparkConf sparkConf = new SparkConf(true).setAppName("Compute primes");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    ArrayList<ArrayList<Point>> AL=new ArrayList<ArrayList<Point>> ();
    int size=(int) (P.size()/Math.sqrt(P.size()));
    int cont=0;
    ArrayList<Point> alp=new ArrayList<Point>();
    for(int i=0;i<P.size();i++)
    {
        alp.add(P.get(i));
        cont++;        
        if(cont>=size)
        {
            AL.add(alp);
            alp=new ArrayList<Point>();
        }
    }
    //groupBy
    JavaRDD< ArrayList<Point>> points = sc.parallelize(AL);
    Clustering C=null;
    
    Broadcast<ArrayList<Point>> broadcast = sc.broadcast(S);
    JavaRDD<Clustering> map = points.map(( Pgrande)->{return ClusteringBuilder.PARTITION(Pgrande, broadcast.getValue(), k);});
    JavaRDD<Cluster> flatMap = map.flatMap(( R)-> {
            //String[] split = "Word".split(" ");
            Cluster[] split=new Cluster[C.getK()];
            return Stream.of(R.getClusters().toArray(split)).iterator();
        });
        JavaPairRDD<Object, Iterable<Cluster>> groupBy = flatMap.groupBy(new Function<Cluster, Object>() {
            @Override
            public Point call(Cluster R) throws Exception {
                return R.getCentroid();
            }
        });
        JavaRDD<Cluster> map1 = groupBy.map(new Function<Tuple2<Object, Iterable<Cluster>>,Cluster>() 
        {
            @Override
            public Cluster call(Tuple2<Object, Iterable<Cluster>> t1) throws Exception 
            {
                Cluster union=new Cluster();
                Iterator<Cluster> iterator = t1._2.iterator();
                while(iterator.hasNext())
                {
                    Cluster next = iterator.next();
                    union=Cluster.union(union,next);
                    
                }
                return union;
            }
           
            });
        Iterator<Cluster> iterator = map1.collect().iterator();
        ArrayList<Cluster> ALC=new ArrayList<Cluster>();
         while(iterator.hasNext())
                {
                    ALC.add(iterator.next());
                    
                }       
            
        return new Clustering(P,ALC,S);    
        }
                
;

}
