package it.unipd.dei.dm1617.test;

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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;

/**
 *
 * @author DavideDP
 */
public class TestKMeansNuovo 
{
    
    public static void main(String[] args) throws FileNotFoundException, IOException, Exception
    {
        testPCA(args);
    }
        //il PCA lo darei per fatto
        //devo testare e completare il kmeansMR_optimized
        //poi farei il kmeansEuristico (sarebbe )
        //poi quella con bisection/Xmeans
        //e anche un ibrido per concludere in bellezza
        //non so quale sia il filtering
        //sembra come il xmeans ma spiegato meglio
        //quindi ci sta
        //magari xmeans è un ibrido tra filter e bisection
        //quindi noi dopo faremo un ibrido usando un ibrido XD
        //ironia
        //ok dai per oggi siamo a posto mi pare
        //domani cosa abbiamo ?
        //Volendo si può lavorare in fondo
        //ok dai sembra che pian piano stia uscendo il progetto
        //AH Manca ancora la gestione dell'input da completare
        //cioè come trasformare documenti in vettori (dava errori nel trasform)
        //e anche allenare meglio il modello
        //OK si è molto incasinato l'argomento sinceramente,
        //sentiamo cosa dice dai
        //OK sì va bene ciao :)
        //BENE         
        
    
    public static void testPCA(String[] args) throws FileNotFoundException, IOException, Exception
    {
        System.out.println("MyFirstTest");
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Workspace\\datamining-project");
        SparkConf sparkConf = new SparkConf(true).setAppName("Compute primes");
        JavaSparkContext sc = new JavaSparkContext(sparkConf); 
        //GESTIONE INPUT IN MANIERA PARALLELA
        JavaRDD<String> textFile = sc.textFile("input.txt");
        JavaRDD<Point> points=textFile.map(
        (doc)->
        {
            String[] split = doc.split("   "); 
            ArrayList<Double> al=new ArrayList<Double>();
            for(String ss:split)
            {
                if(ss.length()!=0)al.add(Double.parseDouble(ss));
            }
            return new PointCentroid(al);        
        }
        );
        
        //STAMPO INFO
        int k=3;
        System.out.println("Total points : "+points.count());
        System.out.println("Total clusters : "+k);
        ArrayList<Vector> vectors=new ArrayList<Vector>();
        for(Point p:points.collect())
        {
            Vector v=p.parseVector();
            vectors.add(v);
            //debugging preventivo XD
            //System.out.println(v);
            //sono 178
        }
        System.out.println("---------------------------");
        ArrayList<Vector> PCAs = Utility.PCA(vectors, sc.sc());
        for(int i=0;i<PCAs.size();i++)
        {
            //System.out.println(vectors.get(i)+"->\n"+PCAs.get(i));
        }
        ArrayList<Point> pointsPCA=new ArrayList<Point>();
        for(Vector v:PCAs)//vectors
        {
            pointsPCA.add(new PointCentroid(v));
        }
        Clustering C = ClusteringBuilder.kmeansAlgorithm(pointsPCA, ClusteringBuilder.getRandomCenters(pointsPCA, k), k);
        //kmeans con centri random ma cmq i punti dipendono dal pca
        //
        File file=new File("output.txt");
        System.out.println(file.getAbsolutePath());
        FileWriter fw = new FileWriter(file);
        for(int i=0;i<C.getK();i++)
        {
            ArrayList<Point> po=C.getClusters().get(i).getPoints();
            for(int j=0;j<po.size();j++)
            {
                Vector parse = po.get(j).parseVector();
                String stam=String.valueOf(parse.apply(0))+" "+String.valueOf(parse.apply(1));
                stam=stam+" "+C.getClusters().get(i).toString();
                fw.write(stam+"\n");
                fw.flush();
            }
        }
        fw.close();
        //in effetti sembra più estesa con il pca
        //che dovrebbe essere un po il suo job ?
        
        
 
        
        
        
    }
    
}
