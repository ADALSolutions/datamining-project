/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617.test;

import it.unipd.dei.dm1617.CentersBuilder;
import it.unipd.dei.dm1617.Clustering;
import it.unipd.dei.dm1617.ClusteringBuilder;
import it.unipd.dei.dm1617.KMeans;
import it.unipd.dei.dm1617.PCA;
import it.unipd.dei.dm1617.Point;
import it.unipd.dei.dm1617.PointSpark;
import it.unipd.dei.dm1617.Utility;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;

/**
 *
 * @author DavideDP
 */
public class TestPCA {

    public static void testPCA_avanzato2(String[] args) throws FileNotFoundException, IOException, Exception {
        System.out.println("MyFirstTest");
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Workspace\\datamining-project");
        SparkConf sparkConf = new SparkConf(true).setAppName("Test PCA");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //GESTIONE INPUT IN MANIERA PARALLELA
        JavaRDD<Point> points = Utility.leggiInput("input13.txt", sc);

        //STAMPO INFO
        int k = 3;
        System.out.println("Total points : " + points.count());
        System.out.println("Total clusters : " + k);
        List<Point> AL = points.collect();
        ArrayList<Point> P = new ArrayList<Point>();
        P.addAll(AL);
        P = PCA.PCAPoints(P, sc.sc(), 13, false, false);
        ArrayList<Point> S = CentersBuilder.getRandomCenters(P, k);//USO STESSO S PER ENTRAMBI
        ArrayList<Point> P7 = PCA.reducePointsDim(P, sc.sc(), 2);
        ArrayList<Point> S7 = PCA.reducePointsDim(S, sc.sc(), 2);
        Clustering C = KMeans.kmeansAlgorithm(P, S, k);
        Clustering C2 = KMeans.kmeansAlgorithm(P7, S7, k);
        C.reduceDim(sc.sc(), 2);
        C2.reduceDim(sc.sc(), 2);
        //Scrivo il clustering
        Utility.writeOuptut("output.txt", C);
        Utility.writeOuptut("output2.txt", C2);
    }

    //serve per valutare se io faccio il clustering su datset con dimensioni=13 e poi faccio il pca per stamparlo in 2d funziona tutto
    //responso:positivo,funziona tutto
    public static void testPCA_avanzato(String[] args) throws FileNotFoundException, IOException, Exception {
        System.out.println("MyFirstTest");
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Workspace\\datamining-project");
        SparkConf sparkConf = new SparkConf(true).setAppName("Test PCA");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //GESTIONE INPUT IN MANIERA PARALLELA
        JavaRDD<Point> points = Utility.leggiInput("input13.txt", sc);

        //STAMPO INFO
        int k = 3;
        System.out.println("Total points : " + points.count());
        System.out.println("Total clusters : " + k);
        List<Point> AL = points.collect();
        ArrayList<Point> P = new ArrayList<Point>();
        P.addAll(AL);
        //Eseguo clustering su dati con D=13
        ArrayList<Point> S = CentersBuilder.getRandomCenters(P, k);//USO STESSO S PER ENTRAMBI
        Clustering C = KMeans.kmeansAlgorithm(P, S, k);
        //Trasformo punti in vettori
        ArrayList<Vector> vectors = new ArrayList<Vector>();
        for (Point p : P) {
            Vector v = p.parseVector();
            vectors.add(v);
        }
        System.out.println("---------------------------");
        //Trasformo vettori con PCA
        ArrayList<Vector> PCAs = PCA.PCA(vectors, sc.sc());
        //Trasformo vettori in punti, lo uso solo se voglio stampare il clustering fatto in 2d
        ArrayList<Point> pointsPCA = new ArrayList<Point>();
        for (Vector v : PCAs)//vectors
        {
            pointsPCA.add(new PointSpark(v));
        }

        //A ogni punto gli associo i punti-2d con il raggruppamento invariato dei clusters
        for (int i = 0; i < P.size(); i++) {

            ((PointSpark) P.get(i)).assignVector(PCAs.get(i));
        }
        //S è stato modificato nel for qui sopra quindi posso usarlo come cenri per il clustering 2d
        Clustering C2 = KMeans.kmeansAlgorithm(pointsPCA, S, k);

        //Scrivo il clustering
        Utility.writeOuptut("output.txt", C);
        Utility.writeOuptut("output2.txt", C2);
    }

    //serve per valutare se il pca funziona correttamente
    public static void testPCA(String[] args) throws FileNotFoundException, IOException, Exception {
        System.out.println("MyFirstTest");
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Workspace\\datamining-project");
        SparkConf sparkConf = new SparkConf(true).setAppName("Test PCA");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //GESTIONE INPUT IN MANIERA PARALLELA
        JavaRDD<Point> points = Utility.leggiInput("input.txt", sc);

        //STAMPO INFO
        int k = 3;
        System.out.println("Total points : " + points.count());
        System.out.println("Total clusters : " + k);
        ArrayList<Vector> vectors = new ArrayList<Vector>();
        for (Point p : points.collect()) {
            Vector v = p.parseVector();
            vectors.add(v);
            //System.out.println(v);
        }
        System.out.println("---------------------------");
        ArrayList<Vector> PCAs = PCA.PCA(vectors, sc.sc());
        for (int i = 0; i < PCAs.size(); i++) {
            //System.out.println(vectors.get(i)+"->\n"+PCAs.get(i));
        }
        ArrayList<Point> pointsPCA = new ArrayList<Point>();
        for (Vector v : PCAs)//vectors
        {
            pointsPCA.add(new PointSpark(v));
        }
        Clustering C = KMeans.kmeansAlgorithm(pointsPCA, CentersBuilder.getRandomCenters(pointsPCA, k), k);
        Utility.writeOuptut("output.txt", C);
    }

}
