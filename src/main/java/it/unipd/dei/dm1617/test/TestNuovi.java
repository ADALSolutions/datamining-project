package it.unipd.dei.dm1617.test;

import it.unipd.dei.dm1617.Cluster;
import it.unipd.dei.dm1617.Clustering;
import it.unipd.dei.dm1617.ClusteringBuilder;
import it.unipd.dei.dm1617.ClusteringBuilderMR;
import it.unipd.dei.dm1617.Point;
import it.unipd.dei.dm1617.PointCentroid;
import it.unipd.dei.dm1617.Utility;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.feature.Word2VecModel;
import scala.Tuple2;


/**
 *
 * @author DavideDP
 */
public class TestNuovi 
{
    
    public static void main(String[] args) throws FileNotFoundException, IOException, Exception 
    {
        
        ArrayList<Point> P = Utility.leggiInputLocale("input.txt");
        
        int k=5;
        //ArrayList<Point> S = Utility.initMedianCenters(P, k);
        ArrayList<Point> S = ClusteringBuilder.getRandomCenters(P, k);
        System.out.println(S.size());
        
        /*testKMeansEuristicoNormale3(P,S,k);
        testKMeansEuristicoNormale3(P,S,k);
        testKMeansEuristicoNormale3(P,S,k);
        testKMeansEuristicoNormale3(P,S,k);
        testKMeansEuristicoNormale3(P,S,k);
        testKMeansEuristicoNormale3(P,S,k);
        testKMeansEuristicoNormale3(P,S,k);
        */
        
        testKMeans2(P,S,k);
        testKMeansEuristico2(P,S,k);
/*
        testKMeansEuristico2(P,S,k);
        testKMeans2(P,S,k);
        testKMeansEuristico2(P,S,k);
        testKMeans2(P,S,k);      
        testKMeansEuristico2(P,S,k);
        testKMeans2(P,S,k);
        testKMeansEuristico2(P,S,k);
        testKMeans2(P,S,k);
        testKMeansEuristico2(P,S,k);
        testKMeans2(P,S,k);  */     
       
    }
        public static void testKMeansEuristicoNormale3(ArrayList<Point> P,ArrayList<Point> S,int k ) throws FileNotFoundException, IOException, Exception 
        {
        long start1,end1,start2,end2;
        
        
        ArrayList<Point> copyP = Utility.copy(P);
        ArrayList<Point> copyS = Utility.copy(S);
        
        
        start1=System.nanoTime();
        Clustering C = ClusteringBuilder.kmeansAlgorithm_old(copyP,copyS , k);
        end1=System.nanoTime();
        System.out.println("Tempo old: "+(end1-start1));        
 
        start2=System.nanoTime();
        Clustering C2 = ClusteringBuilder.kmeansAlgorithm_old(P, S, k);
        end2=System.nanoTime();
        System.out.println("Tempo nuovo: "+(end2-start2));   
        
        
        
        //C.reduceDim(sc.sc(),2);
        //C2.reduceDim(sc.sc(),2);
        System.out.println("old: "+C.kmeans());
        System.out.println("nuovo: "+C2.kmeans());
        System.out.println("Ratio: "+C.kmeans()/C2.kmeans());            
        }

            
    public static void testKMeans2(ArrayList<Point> P,ArrayList<Point> S,int k ) throws FileNotFoundException, IOException, Exception {

        int cont=0;
        double sumPhi=0;
        double sumTime=0;
        while(cont<10)
        {
            long start1,end1,start2,end2;
            ArrayList<Point> copyP = Utility.copy(P);
            ArrayList<Point> copyS = Utility.copy(S);

            start2=System.currentTimeMillis();
            Clustering C2 = ClusteringBuilder.kmeansAlgorithm_old(P, S, k);
            end2=System.currentTimeMillis();
            sumPhi+=C2.kmeans();
            sumTime+=(end2-start2);

            cont++;
        }
           System.out.println("Tempo KMeans: "+(sumTime/10));   
           System.out.println("Classico: "+(sumPhi/10));
    }    
    public static void testKMeansEuristico2(ArrayList<Point> P,ArrayList<Point> S,int k) throws FileNotFoundException, IOException, Exception {

        int cont=0;
        double sumPhi=0;
        double sumTime=0;
        while(cont<10)
        {
            long start1,end1,start2,end2;
            ArrayList<Point> copyP = Utility.copy(P);
            ArrayList<Point> copyS = Utility.copy(S); 

            start1=System.currentTimeMillis();
            Clustering C = ClusteringBuilder.kmeansEuristic_old(copyP,copyS , k);
            end1=System.currentTimeMillis();
            sumPhi+=C.kmeans();
            sumTime+=(end1-start1);
            cont++;
        }
           System.out.println("Tempo KMeansEuristico: "+(sumTime/10));   
           System.out.println("Euristico: "+(sumPhi/10));
    }        
    public static void testKMeansEuristico(String[] args) throws FileNotFoundException, IOException, Exception {
        System.out.println("KMEANSEURISTICO");
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Workspace\\datamining-project");
        SparkConf sparkConf = new SparkConf(true).setAppName("Test PCA");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<Point> points = Utility.leggiInput("Iris.txt", sc);
        int k=5;
        List<Point> coll = points.collect();
        ArrayList<Point> P=new ArrayList<Point>(coll.size());
        P.addAll(coll);
        //P= Utility.PCAPoints(P, sc.sc(), 13,true, true);
        //ArrayList<Point> S = Utility.initMedianCenters(P, k);
        ArrayList<Point> S = ClusteringBuilder.getRandomCenters(P, k);
        System.out.println(S.size());
        long start1,end1,start2,end2;
        
        
        ArrayList<Point> copyP = Utility.copy(P);
        ArrayList<Point> copyS = Utility.copy(S);
        
        
        start1=System.currentTimeMillis();
        Clustering C = ClusteringBuilder.kmeansAlgorithm_old(copyP,copyS , k);
        end1=System.currentTimeMillis();
        System.out.println("Tempo old: "+(end1-start1));        
 
        start2=System.currentTimeMillis();
        Clustering C2 = ClusteringBuilder.kmeansAlgorithm_old(P, S, k);
        end2=System.currentTimeMillis();
        System.out.println("Tempo nuovo: "+(end2-start2));   
        
        
        
        //C.reduceDim(sc.sc(),2);
        //C2.reduceDim(sc.sc(),2);
        System.out.println("Euristico: "+C.kmeans());
        System.out.println("Classico: "+C2.kmeans());
        System.out.println("Ratio: "+C.kmeans()/C2.kmeans());
        //Utility.writeOuptut("output.txt", C);
        //Utility.writeOuptut("output.txt", C2);
    }    

    public static void testSize(String[] args) {
        double p[] = {1, 2, 5, 6, 7, 8, 9, 3, 5, 6, 9, 6, 12, 53, 56, 3456, 754, 345, 8765, 456};
        //for(int i=0;)
        System.out.println("dimensione : " + p.length);
        System.out.println("Size dell'array double : " + ObjectSizeCalculator.getObjectSize(p));
        ArrayList<Double> a = new ArrayList<Double>();
        for (int i = 0; i < p.length; i++) {
            a.add((double) p[i]);
        }
        PointCentroid point = new PointCentroid(a);
        System.out.println("Size del Point: " + ObjectSizeCalculator.getObjectSize(point));
        System.out.println("Size del Vector di spark : " + ObjectSizeCalculator.getObjectSize(point.parseVector()));
        //System.out.println("Size dell' ArrayList : "+ObjectSizeCalculator.getObjectSize(point.point));
    }

}
