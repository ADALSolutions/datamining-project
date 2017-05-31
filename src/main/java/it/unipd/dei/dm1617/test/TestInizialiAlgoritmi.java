
package it.unipd.dei.dm1617.test;
import it.unipd.dei.dm1617.*;
import java.io.File;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
/**
 *
 * @author DavideDP
 */
public class TestInizialiAlgoritmi 
{
    public static void main(String[] args) throws FileNotFoundException, IOException
    {
        long start = System.currentTimeMillis();
        //Test.KCenterMain(args);
        TestInizialiAlgoritmi.KCenterMainRDD(args);
        long end = System.currentTimeMillis();
        System.out.println("TIME: "+(end-start)/100);
        
    }
    public static void KMeansRDD(String[] args) throws FileNotFoundException, IOException
    {
        
    }
    public static void KCenterMainRDD(String[] args) throws FileNotFoundException, IOException
    {
        System.out.println("MyFirstTest");
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Workspace\\datamining-project");
        SparkConf sparkConf = new SparkConf(true).setAppName("Compute primes");
        JavaSparkContext sc = new JavaSparkContext(sparkConf); 
        //GESTIONE INPUT IN MANIERA PARALLELA
        JavaRDD<Point> points=Utility.leggiInput("input.txt", sc);
        
        //STAMPO INFO
        int k=6;
        System.out.println("Total points : "+points.count());
        System.out.println("Total clusters : "+k);
        //CREO PUNTI RANDOM S
        long count = points.count();
        int size=(int)(((double)count)/Math.sqrt(count));
        JavaRDD<Point> S2 = points.sample(false,((double)2*k)/count);  
        ArrayList<Point> S3 =new ArrayList( S2.collect());
        ArrayList<Point> S=new ArrayList<Point> ();
        for(int i=0;i<k;i++)
        {
            S.add(S3.get(i));
        }        
        //FACCIO PARTITION IN PARALLELO
        Broadcast<ArrayList<Point>> broadcast = sc.broadcast(S);
        ArrayList<Point> P=new ArrayList<Point>(points.collect());
        ArrayList<Cluster> PARTITION = ClusteringBuilderMR.PartitionMR(points, broadcast, k);
        Clustering C=new Clustering(P,PARTITION);
        int sum=0;
        for(int i=0;i<C.getK();i++)
        {
            System.out.println(C.getClusters().get(i).toString()+" : "+C.getClusters().get(i).size());
            sum+=C.getClusters().get(i).size();
        }
        double kcenter = C.kcenter();
        System.out.println("phi: "+kcenter);
        System.out.println("sum: "+sum);       
        
    }        
    public static void KMeansMain(String[] args) throws FileNotFoundException, IOException
    {
        System.out.println("MyFirstTest");
        ArrayList<Point> points=Utility.leggiInputLocale("input.txt");
        System.out.println("Total points : "+points.size());
        int k=6;
        System.out.println("Total clusters : "+k);
        ArrayList<Point> S = ClusteringBuilder.getRandomCenters(points,k);
        Clustering C = ClusteringBuilder.kmeansAlgorithm(points,S, k);
        int sum=0;
        for(int i=0;i<C.getK();i++)
        {
            System.out.println(C.getClusters().get(i).toString()+" : "+C.getClusters().get(i).size());
            sum+=C.getClusters().get(i).size();
        }
        double kmeans = C.kmeans()/points.size();
        System.out.println("phi: "+kmeans);
        System.out.println("sum: "+sum);
        //stampa su file
        Utility.writeOuptut("output.txt", C);
        
    }
    public static void KCenterMain(String[] args) throws FileNotFoundException, IOException
    {
        System.out.println("MyFirstTest");
        //A-Sets:A1 Synthetic 2-d data with varying number of vectors (N) and clusters (M). There are 150 vectors per cluster.
        ArrayList<Point> points = Utility.leggiInputLocale("input.txt");
        System.out.println("Total points : "+points.size());
        int k=6;
        System.out.println("Total clusters : "+k);
        //ArrayList<Point> S = ClusteringBuilder.getRandomCenters(points,k);
        //Clustering C = ClusteringBuilder.PARTITION(points, S, k);
        Clustering C = ClusteringBuilder.FarthestFirstTraversal(points, k);
        int sum=0;
        for(int i=0;i<C.getK();i++)
        {
            System.out.println(C.getClusters().get(i).toString()+" : "+C.getClusters().get(i).size());
            sum+=C.getClusters().get(i).size();
        }
        double kcenter = C.kcenter();
        System.out.println("phi: "+kcenter);
        System.out.println("sum: "+sum);
        //stampa su file
        Utility.writeOuptut("output.txt", C);
   
    }    
    
    public static void KCenterMainRDD_old(String[] args) throws FileNotFoundException, IOException
    {
        System.out.println("MyFirstTest");
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Workspace\\datamining-project");
        SparkConf sparkConf = new SparkConf(true).setAppName("Compute primes");
        JavaSparkContext sc = new JavaSparkContext(sparkConf); 
        JavaRDD<Point> points=Utility.leggiInput("input.txt", sc);
        int letT=1;
        long count = points.count();
        ArrayList<Point> P=new ArrayList<Point>(points.collect());
        int k=6;
        int size=(int)(((double)count)/Math.sqrt(count));
        JavaRDD<Point> S2 = points.sample(false,((double)2*k)/count);  
        ArrayList<Point> S3 =new ArrayList( S2.collect());
        ArrayList<Point> S=new ArrayList<Point> ();
        for(int i=0;i<k;i++)
        {
            S.add(S3.get(i));
        }
        
        JavaPairRDD<Integer, Iterable<Point>> groups = points.groupBy((p)->{return Integer.parseInt(p.getID())%size; });
        System.out.println("Total points : "+count);
        System.out.println("Total clusters : "+k);
        Broadcast<ArrayList<Point>> broadcast = sc.broadcast(S);
        JavaRDD<Clustering> map = groups.map((Tuple2<Integer, Iterable<Point>> tupla) -> {
            Iterator<Point> it=tupla._2.iterator();
            ArrayList<Point> punti=new ArrayList<Point>();
            while(it.hasNext()){punti.add(it.next());}
            //System.out.println("size:"+broadcast.getValue()+"     k:"+k);
            return ClusteringBuilder.Partition(punti, broadcast.getValue(), k) ;
        });
        JavaRDD<Cluster> flatMap = map.flatMap((Clustering C)->{

            int sum=0;
            
            for(int i=0;i<C.getK();i++)
            {
                System.out.println(C.getClusters().get(i).toString()+" : "+C.getClusters().get(i).size());
                sum+=C.getClusters().get(i).size();
            }    
            System.out.println("-----------------------------------------");
           
            return C.getClusters().iterator();});
        JavaPairRDD<Vector, Iterable<Cluster>> groupBy = flatMap.groupBy((C)->{return C.getCenter().parseVector();});
        JavaRDD<Cluster> map1 = groupBy.map(  (Tupla)->{
            Iterator<Cluster> it=Tupla._2.iterator();
            Cluster union=new Cluster();
            while(it.hasNext()){Cluster.union(union, it.next());}
            return union;
        }     );
        ArrayList<Cluster> clusters = new ArrayList<Cluster>( map1.collect());
        Clustering C=new Clustering(P,clusters);
        int sum=0;
        for(int i=0;i<C.getK();i++)
        {
            System.out.println(C.getClusters().get(i).toString()+" : "+C.getClusters().get(i).size());
            sum+=C.getClusters().get(i).size();
        }
        double kcenter = C.kcenter();
        System.out.println("phi: "+kcenter);
        System.out.println("sum: "+sum);       
        
    }        
    
}