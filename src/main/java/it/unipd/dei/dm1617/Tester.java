/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import it.unipd.dei.dm1617.Clustering;
import it.unipd.dei.dm1617.Point;
import it.unipd.dei.dm1617.Utility;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

/**
 *
 * @author DavideDP
 */
public class Tester {

    public static void template(String[] args) 
    {
        String nomeMetodo1="Normale ";
        String nomeMetodo2="Euristico";
        ArrayList<Point> P = Tester.caricoSpark("Confronto tra "+nomeMetodo1+" e "+nomeMetodo2,"Iris.txt");  
        int k=10;
        int numIterations=10;
        Tuple4<Double, Double, Double, Double> test = 
        Tester.testIteratoConfronto
        (
                 P,k,numIterations,
                (P2,k2)->{return CentersBuilder.initMedianCenters(P2, k2)  ;},//ClusteringBuilder.kmeansPlusPlus(P2, k2)  ClusteringBuilder.getRandomCenters(P2, k2)
                //Corrisponde a P,S,k da passare all'algoritmo da testare specifico
                (t)->{return KMeans.kmeansAlgorithm(t._1(), t._2(), t._3());},
                (t)->{return KMeans.kmeansEuristic(t._1(), t._2(), t._3());},
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
    
        
    public static Tuple2<Double, Double> testIterato(ArrayList<Point> P, //Scegle centri
    BiFunction<ArrayList<Point>, Integer, ArrayList<Point>> chooseS, //Corrisponde a P,S,k da passare all'algoritmo da testare specifico
    Function<Tuple3<ArrayList<Point>, ArrayList<Point>, Integer>, Clustering> funcCreateClustering, //Scelgo lo score del clustering
    Function<Clustering, Double> funcScore) {
        int length = 10;
        int tot = 30;
        int k = 5;
        double sumTimeTot = 0;
        double sumScoreTot = 0;
        int i = 0;
        while (i < tot) {
            double sumTime = 0;
            double sumScore = 0;
            int j = 0;
            while (j < length) {
                ArrayList<Point> S = chooseS.apply(P, k);
                Tuple2<Double, Double> t = testAlgoritmo(P, S, k, funcCreateClustering, funcScore);
                sumTime += t._1;
                sumScore += t._2;
                j++;
            }
            sumTime /= length;
            sumScore /= length;
            sumTimeTot += sumTime;
            sumScoreTot += sumScore;
            i++;
        }
        sumTimeTot /= tot;
        sumScoreTot /= tot;
        return new Tuple2<Double, Double>(sumTimeTot, sumScoreTot);
    }

    public static Tuple4<Double, Double, Double, Double> testIteratoConfronto(ArrayList<Point> P, int k, int numIterations, //Scegle centri
    BiFunction<ArrayList<Point>, Integer, ArrayList<Point>> chooseS, //Corrisponde a P,S,k da passare all'algoritmo da testare specifico
    Function<Tuple3<ArrayList<Point>, ArrayList<Point>, Integer>, Clustering> funcCreateClustering, Function<Tuple3<ArrayList<Point>, ArrayList<Point>, Integer>, Clustering> funcCreateClustering2, //Scelgo lo score del clustering
    Function<Clustering, Double> funcScore) {
        int length = 10;
        double sumTimeTot = 0;
        double sumScoreTot = 0;
        double sumTimeTot2 = 0;
        double sumScoreTot2 = 0;
        int i = 0;
        int tot = numIterations;
        while (i < tot) {
            double sumTime = 0;
            double sumScore = 0;
            double sumTime2 = 0;
            double sumScore2 = 0;
            int j = 0;
            while (j < length) {
                System.out.println("Iterazione:" + (i * length + j));
                ArrayList<Point> S = chooseS.apply(P, k);
                Tuple2<Double, Double> t = testAlgoritmo(P, S, k, funcCreateClustering, funcScore);
                sumTime += t._1;
                sumScore += t._2;
                t = testAlgoritmo(P, S, k, funcCreateClustering2, funcScore);
                sumTime2 += t._1;
                sumScore2 += t._2;
                j++;
            }
            sumTime /= length;
            sumScore /= length;
            sumTimeTot += sumTime;
            sumScoreTot += sumScore;
            sumTime2 /= length;
            sumScore2 /= length;
            sumTimeTot2 += sumTime2;
            sumScoreTot2 += sumScore2;
            i++;
        }
        sumTimeTot /= tot;
        sumScoreTot /= tot;
        sumTimeTot2 /= tot;
        sumScoreTot2 /= tot;
        return new Tuple4<Double, Double, Double, Double>(sumTimeTot, sumScoreTot, sumTimeTot2, sumScoreTot2);
    }

    public static Tuple2<Double, Double> testAlgoritmo(ArrayList<Point> P, ArrayList<Point> S, int k, //Corrisponde a P,S,k da passare all'algoritmo da testare specifico
    Function<Tuple3<ArrayList<Point>, ArrayList<Point>, Integer>, Clustering> funcCreateClustering, //Scelgo lo score del clustering
    Function<Clustering, Double> funcScore) {
        ArrayList<Point> copyP = Utility.copy(P);
        ArrayList<Point> copyS = Utility.copy(S);
        long start = System.currentTimeMillis();
        Clustering C = funcCreateClustering.apply(new Tuple3(copyP, copyS, k));
        long end = System.currentTimeMillis();
        double diff = end - start;
        double score = funcScore.apply(C);
        Tuple2<Double, Double> t = new Tuple2(diff, score);
        return t;
    }

    public static ArrayList<Point> caricoSpark(String appName, String file) {
        //CARICO SPARK
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Workspace\\datamining-project");
        SparkConf sparkConf = new SparkConf(true).setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //LEGGO INPUT
        JavaRDD<Point> points = Utility.leggiInput(file, sc);
        List<Point> coll = points.collect();
        ArrayList<Point> P = new ArrayList<Point>(coll.size());
        P.addAll(coll);
        return P;
    }
    
}
