/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617.test;

import it.unipd.dei.dm1617.Clustering;
import it.unipd.dei.dm1617.ClusteringBuilder;
import it.unipd.dei.dm1617.Point;
import it.unipd.dei.dm1617.Utility;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple4;

/**
 *
 * @author DavideDP
 */
public class TestAggiornati 
{
    public static void main(String[] args) throws FileNotFoundException, IOException, Exception 
    {
        testIteratoConfrontoKMeansNormale();
        
    }    
    public static void testIteratoConfrontoKMeansNormaleEuristico() throws FileNotFoundException, IOException, Exception 
    {
        int length=100;
        int k=5;
        Tuple2<ArrayList<Point>, ArrayList<Point>> t = caricoSpark(k);
        ArrayList<Point>    S=t._2;
        ArrayList<Point>    P=t._1;
        int i=0;
        double sumPhiOld=0;
        double sumPhiNuovo=0;
        double sumTimeOld=0;
        double sumTimeNuovo=0;
        while(i<length)
        {
            //<tempoOld,tempoNuovo,QualOld,QualNuovo>
            Tuple4<Integer, Integer, Double, Double> t4 = testConfrontoKmeans(false,P,S,k);
            i++;
            sumTimeOld+=t4._1();
            sumTimeNuovo+=t4._2();
            sumPhiOld+=t4._3();
            sumPhiNuovo+=t4._4();
        }
        System.out.println("Tempo Normale: "+sumTimeOld/length);
        System.out.println("Tempo Euristico: "+sumTimeNuovo/length);
        System.out.println("Qualità Normale: "+sumPhiOld/length);
        System.out.println("Qualità Euristico: "+sumPhiNuovo/length);
        
    }
    public static void testIteratoConfrontoKMeansNormale() throws FileNotFoundException, IOException, Exception 
    {
        int length=100;
        int k=5;
        Tuple2<ArrayList<Point>, ArrayList<Point>> t = caricoSpark(k);
        ArrayList<Point>    S=t._2;
        ArrayList<Point>    P=t._1;
        int i=0;
        double sumPhiOld=0;
        double sumPhiNuovo=0;
        double sumTimeOld=0;
        double sumTimeNuovo=0;
        while(i<length)
        {
            //<tempoOld,tempoNuovo,QualOld,QualNuovo>
            Tuple4<Integer, Integer, Double, Double> t4 = testConfrontoKmeans(false,P,S,k);
            i++;
            sumTimeOld+=t4._1();
            sumTimeNuovo+=t4._2();
            sumPhiOld+=t4._3();
            sumPhiNuovo+=t4._4();
        }
        System.out.println("Tempo old: "+sumTimeOld/length);
        System.out.println("Tempo nuovo: "+sumTimeNuovo/length);
        System.out.println("Qualità old: "+sumPhiOld/length);
        System.out.println("Qualità Nuova: "+sumPhiNuovo/length);
        
    }
    public static Tuple2< ArrayList<Point>, ArrayList<Point>> caricoSpark(int k)
    {
        //CARICO SPARK
        System.out.println("KMEANS");
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Workspace\\datamining-project");
        SparkConf sparkConf = new SparkConf(true).setAppName("Test PCA");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        //LEGGO INPUT
        JavaRDD<Point> points = Utility.leggiInput("Input.txt", sc);
        List<Point> coll = points.collect();
        ArrayList<Point> P=new ArrayList<Point>(coll.size());
        P.addAll(coll);
        
        //SCELGO CENTRI
        //P= Utility.PCAPoints(P, sc.sc(), 13,true, true);
        //ArrayList<Point> S = Utility.initMedianCenters(P, k);
        ArrayList<Point> S = ClusteringBuilder.getRandomCenters(P, k);
        System.out.println(S.size());  
        return new Tuple2(P,S);
    }
    //<tempoNormale,tempoEuristico,QualNormale,QualEuristico>
    public static Tuple4<Integer,Integer,Double,Double> testConfrontoKmeansNormaleEuristico(boolean init,ArrayList<Point> P, ArrayList<Point> S,int k) throws FileNotFoundException, IOException, Exception 
    {
        if(init)
        {
            Tuple2<ArrayList<Point>, ArrayList<Point>> t = caricoSpark(k);
            S=t._2;
            P=t._1;
        }
        long start1,end1,start2,end2;
        
        //Copio dati
        ArrayList<Point> copyP = Utility.copy(P);
        ArrayList<Point> copyS = Utility.copy(S);
        
        //Eseguo KMEANS VECCHIO(OGNI VOLTA CREO NUOVO CLUSTER)
        start1=System.currentTimeMillis();
        Clustering CNormale = ClusteringBuilder.kmeansAlgorithm(copyP,copyS , k);
        end1=System.currentTimeMillis();
 
        //Eseguo KMEANS VECCHIO(FACCIO UPDATE AL CLUSTERING)
        start2=System.currentTimeMillis();
        Clustering Ceuristic = ClusteringBuilder.kmeansEuristic(P, S, k);
        end2=System.currentTimeMillis();

        //STAMPO INFORMAZIONI COME TEMPO E QUALITA'
        /*
        System.out.println("Tempo old: "+(end1-start1));               
        System.out.println("Tempo nuovo: "+(end2-start2));   
        System.out.println("Euristico: "+Cold.kmeans());
        System.out.println("Classico: "+Cnuovo.kmeans());
        System.out.println("Ratio: "+Cold.kmeans()/Cnuovo.kmeans());
        */
        Tuple4<Integer,Integer,Double,Double> t=new Tuple4((int)(end1-start1),(int)(end2-start2),CNormale.kmeans(),Ceuristic.kmeans());
        return t;
    }      
    //<tempoOld,tempoNuovo,QualOld,QualNuovo>
    public static Tuple4<Integer,Integer,Double,Double> testConfrontoKmeans(boolean init,ArrayList<Point> P, ArrayList<Point> S,int k) throws FileNotFoundException, IOException, Exception 
    {
        if(init)
        {
            Tuple2<ArrayList<Point>, ArrayList<Point>> t = caricoSpark(k);
            S=t._2;
            P=t._1;
        }
        long start1,end1,start2,end2;
        
        //Copio dati
        ArrayList<Point> copyP = Utility.copy(P);
        ArrayList<Point> copyS = Utility.copy(S);
        
        //Eseguo KMEANS VECCHIO(OGNI VOLTA CREO NUOVO CLUSTER)
        start1=System.currentTimeMillis();
        Clustering Cold = ClusteringBuilder.kmeansAlgorithm_old(copyP,copyS , k);
        end1=System.currentTimeMillis();
 
        //Eseguo KMEANS VECCHIO(FACCIO UPDATE AL CLUSTERING)
        start2=System.currentTimeMillis();
        Clustering Cnuovo = ClusteringBuilder.kmeansAlgorithm(P, S, k);
        end2=System.currentTimeMillis();

        //STAMPO INFORMAZIONI COME TEMPO E QUALITA'
        /*
        System.out.println("Tempo old: "+(end1-start1));               
        System.out.println("Tempo nuovo: "+(end2-start2));   
        System.out.println("Euristico: "+Cold.kmeans());
        System.out.println("Classico: "+Cnuovo.kmeans());
        System.out.println("Ratio: "+Cold.kmeans()/Cnuovo.kmeans());
        */
        Tuple4<Integer,Integer,Double,Double> t=new Tuple4((int)(end1-start1),(int)(end2-start2),Cold.kmeans(),Cnuovo.kmeans());
        return t;
    }      
}
