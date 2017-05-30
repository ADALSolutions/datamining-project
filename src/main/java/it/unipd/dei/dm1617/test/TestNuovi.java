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
public class TestNuovi {

    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        testPCA_avanzato(args);
    }
    
    //serve per valutare se io faccio il clustering su datset con dimensioni=13 e poi faccio il pca per stamparlo in 2d funziona tutto
    //responso:positivo,funziona tutto
    public static void testPCA_avanzato(String[] args) throws FileNotFoundException, IOException, Exception {
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
        List<Point> AL=points.collect();
        ArrayList<Point> P=new ArrayList<Point>();
        P.addAll(AL);
        //Eseguo clustering su dati con D=13
        ArrayList<Point> S = ClusteringBuilder.getRandomCenters(P, k);//USO STESSO S PER ENTRAMBI
        Clustering C = ClusteringBuilder.kmeansAlgorithm(P, S, k);
       //Trasformo punti in vettori
        ArrayList<Vector> vectors = new ArrayList<Vector>();
        for (Point p : P) {
            Vector v = p.parseVector();
            vectors.add(v);
        }
        System.out.println("---------------------------");
        //Trasformo vettori con PCA
        ArrayList<Vector> PCAs = Utility.PCA(vectors, sc.sc());
        //Trasformo vettori in punti, lo uso solo se voglio stampare il clustering fatto in 2d
        ArrayList<Point> pointsPCA = new ArrayList<Point>();
        for (Vector v : PCAs)//vectors
        {
            pointsPCA.add(new PointCentroid(v));
        }

        //A ogni punto gli associo i punti-2d con il raggruppamento invariato dei clusters
        for (int i=0;i<P.size();i++) 
        {
            
            ((PointCentroid)P.get(i)).assignVector(PCAs.get(i));
        }   
        //S è stato modificato nel for qui sopra quindi posso usarlo come cenri per il clustering 2d
        Clustering C2 = ClusteringBuilder.kmeansAlgorithm(pointsPCA, S, k);
        
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
        ArrayList<Vector> PCAs = Utility.PCA(vectors, sc.sc());
        for (int i = 0; i < PCAs.size(); i++) {
            //System.out.println(vectors.get(i)+"->\n"+PCAs.get(i));
        }
        ArrayList<Point> pointsPCA = new ArrayList<Point>();
        for (Vector v : PCAs)//vectors
        {
            pointsPCA.add(new PointCentroid(v));
        }
        Clustering C = ClusteringBuilder.kmeansAlgorithm(pointsPCA, ClusteringBuilder.getRandomCenters(pointsPCA, k), k);
        Utility.writeOuptut("output.txt", C);
    }

    
    public static void testSinonimi(String[] args) throws FileNotFoundException, IOException, InterruptedException {

        System.out.println("Sample");
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Esempi\\dm1617-project-stub");
        SparkConf conf = new SparkConf(true).setAppName("Sampler");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //sc.sc().setLogLevel("ERROR");

        String inputPath = "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Esempi\\dm1617-project-stub\\medium-sample.dat.bz2";          //medium-sample.dat.bz2
        String outputPath = "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Esempi\\dm1617-project-stub\\output";
        String modelPath = "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Esempi\\dm1617-project-stub\\model\\Word2Vec";
        double fraction = 0.1;

        Word2VecModel load = Word2VecModel.load(sc.sc(), modelPath);
        System.out.println("Modello caricato");
        Broadcast<Word2VecModel> b = sc.broadcast(load);
        Vector transform = load.transform(load.org$apache$spark$mllib$feature$Word2VecModel$$wordList()[0]);
        int size = transform.size();
        System.out.println("size:" + String.valueOf(size));
        String[] wordList = load.org$apache$spark$mllib$feature$Word2VecModel$$wordList();
        System.out.println("size word list:" + String.valueOf(wordList.length));
        /*for(String s:wordList)
      {
      System.out.println(s);
      }*/
        Tuple2<String, Object>[] mappiamo = load.findSynonyms("garden", 10);

        for (Tuple2<String, Object> t : mappiamo) {
            System.out.println(t._1 + " : " + String.valueOf(t._2) + " " + load.transform("botanic"));
            //System.out.println("Sample");
        }
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
