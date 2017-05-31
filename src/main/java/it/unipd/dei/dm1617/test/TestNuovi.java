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
import java.util.concurrent.TimeUnit;
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
    public static void main(String[] args) throws FileNotFoundException, IOException, Exception 
    {
        /*System.out.println("KMEANS");
        ArrayList<Point> P = Utility.leggiInputLocale("Iris.txt");
        int k=5;
       //P= Utility.PCAPoints(P, null, 13,true, true);
        //ArrayList<Point> S = Utility.initMedianCenters(P, k);
        ArrayList<Point> S = ArrayList<Point>(5);
        S.add(P.get(5));S.add(P.get(29));S.add(P.get(70));S.add(P.get(56));
        System.out.println(S.size());
        //TimeUnit.SECONDS.sleep(100);
        testKMeansEuristico2(P,S,k);
        testKMeansEuristico2(P,S,k);
        testKMeansEuristico2(P,S,k);
        testKMeansEuristico2(P,S,k);
        */
        testTempi(args);
    }
    public static void testTempi(String[] args) throws FileNotFoundException, IOException, Exception 
    {
        System.out.println("KMEANS");
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Workspace\\datamining-project");
        SparkConf sparkConf = new SparkConf(true).setAppName("Test PCA");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<Point> points = Utility.leggiInput("Iris.txt", sc);
        int k=20;
        List<Point> coll = points.collect();
        ArrayList<Point> P=new ArrayList<Point>(coll.size());
        P.addAll(coll);
        //P= Utility.PCAPoints(P, sc.sc(), 13,true, true);
        //ArrayList<Point> S = Utility.initMedianCenters(P, k);
        ArrayList<Point> S = ClusteringBuilder.getRandomCenters(P, k);
        System.out.println(S.size());
        //Monitor mon=MonitorFactory.start("myFirstMonitor");
        testKMeans2(P,S,k);
        testKMeansEuristico2(P,S,k);
        testKMeans2(P,S,k);
        testKMeansEuristico2(P,S,k);
        testKMeans2(P,S,k);
        testKMeansEuristico2(P,S,k);
        testKMeans2(P,S,k);
        testKMeansEuristico2(P,S,k);
        testKMeans2(P,S,k);
        testKMeansEuristico2(P,S,k);
        testKMeans2(P,S,k);
        testKMeansEuristico2(P,S,k);
       
        testKMeans2(P,S,k);
        testKMeansEuristico2(P,S,k);
        testKMeans2(P,S,k);
        testKMeansEuristico2(P,S,k);
        testKMeansEuristico2(P,S,k);
    }
    //serve per valutare se io faccio il clustering su datset con dimensioni=13 e poi faccio clustering con d=7 e poi stampando su d=2 ottengo gli stessi risultati/gruppi
    
    
    public static void testKMeans2(ArrayList<Point> P,ArrayList<Point> S,int k ) throws FileNotFoundException, IOException, Exception {

        int cont=0;
        double sumPhi=0;
        double sumTime=0;
        while(cont<100)
        {
            long start1,end1,start2,end2;
            ArrayList<Point> copyP = Utility.copy(P);
            ArrayList<Point> copyS = Utility.copy(S);

            start2=System.currentTimeMillis();
            Clustering C2 = ClusteringBuilder.kmeansAlgorithm(P, S, k);
            end2=System.currentTimeMillis();
            sumPhi+=C2.kmeans();
            sumTime+=end2-start2;

            cont++;
        }
           System.out.println("Tempo KMeans: "+(sumTime/100));   
           System.out.println("Classico: "+(sumPhi/100));
    }    
    public static void testKMeansEuristico2(ArrayList<Point> P,ArrayList<Point> S,int k) throws FileNotFoundException, IOException, Exception {

        int cont=0;
        double sumPhi=0;
        double sumTime=0;
        while(cont<100)
        {
            long start1,end1,start2,end2;
            ArrayList<Point> copyP = Utility.copy(P);
            ArrayList<Point> copyS = Utility.copy(S); 

            start1=System.currentTimeMillis();
            Clustering C = ClusteringBuilder.kmeansEuristic(copyP,copyS , k);
            end1=System.currentTimeMillis();
            sumPhi+=C.kmeans();
            sumTime+=end1-start1;
            cont++;
        }
           System.out.println("Tempo KMeansEuristico: "+(sumTime/100));   
           System.out.println("Euristico: "+(sumPhi/100));
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

        start2=System.currentTimeMillis();
        Clustering C2 = ClusteringBuilder.kmeansAlgorithm(P, S, k);
        end2=System.currentTimeMillis();
        System.out.println("Tempo KMeans: "+(end2-start2));   
        
        
        start1=System.currentTimeMillis();
        Clustering C = ClusteringBuilder.kmeansEuristic(copyP,copyS , k);
        end1=System.currentTimeMillis();
        System.out.println("Tempo Euristico: "+(end1-start1));        
        
        //C.reduceDim(sc.sc(),2);
        //C2.reduceDim(sc.sc(),2);
        System.out.println("Euristico: "+C.kmeans());
        System.out.println("Classico: "+C2.kmeans());
        System.out.println("Ratio: "+C.kmeans()/C2.kmeans());
        //Utility.writeOuptut("output.txt", C);
        //Utility.writeOuptut("output.txt", C2);
    }

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
        List<Point> AL=points.collect();
        ArrayList<Point> P=new ArrayList<Point>();
        P.addAll(AL);
        P= Utility.PCAPoints(P, sc.sc(), 13,false, false);
        ArrayList<Point> S = ClusteringBuilder.getRandomCenters(P, k);//USO STESSO S PER ENTRAMBI
        ArrayList<Point> P7=Utility.reducePointsDim(P, sc.sc(), 2);
        ArrayList<Point> S7=Utility.reducePointsDim(S, sc.sc(), 2);
        Clustering C = ClusteringBuilder.kmeansAlgorithm(P, S, k);
        Clustering C2 = ClusteringBuilder.kmeansAlgorithm(P7, S7, k);
        C.reduceDim(sc.sc(),2);
        C2.reduceDim(sc.sc(),2);
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
