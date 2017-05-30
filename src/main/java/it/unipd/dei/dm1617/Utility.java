/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import breeze.linalg.DenseMatrix;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author DavideDP
 */
public class Utility {

    //scrive nuovi centroidi dentro al clustering
    public static void initRandomCentroids(Clustering primeClustering, int k) {
        for (int i = 0; i <= k - 1; i++) {
            Point centroid = primeClustering.getRandom();
            primeClustering.getCenters().add(centroid);
        }
    }
    public static List<Vector> toArrayList(double[][] array )
    {
        LinkedList<Vector> rowsList = new LinkedList<Vector>();
        for (int i = 0; i < array.length; i++) {
          Vector currentRow = Vectors.dense(array[i]);
          rowsList.add(currentRow);
        }
        return rowsList;
    }
    public static ArrayList toArrayList(double[] dd) {
        ArrayList v = new ArrayList(dd.length);
        for (int i = 0; i < dd.length; i++) {
            v.add(dd[i]);
        }
        return v;
    }

    //posizione nell'array del min e il riferimento all'oggetto
    public static Tuple2<Integer, Point> mostClose(ArrayList<Point> P, Point c) {
        Point p = null;
        double min = Double.MAX_VALUE;
        int argmin = -1;
        for (int i = 0; i < P.size(); i++) {
            double dist = Distance.calculateDistance(P.get(i).parseVector(), c.parseVector(), "standard");
            if (dist <= min && P.get(i).equals(p) == false) {
                p = P.get(i);
                min = dist;
                argmin = i;
            }
        }
        return new Tuple2(argmin, p);
    }

    public static Tuple2<Integer, Point> mostClose(ArrayList<Point> P, ArrayList<Point> S) {
        Point p = null;
        double min = Double.MAX_VALUE;
        int argmin = -1;
        for (int i = 0; i < S.size(); i++) {
            Tuple2<Integer, Point> t = Utility.mostClose(P, S.get(i));
            double dist = Distance.calculateDistance(t._2.parseVector(), S.get(i).parseVector(), "standard");
            if (dist <= min && P.get(i).equals(p) == false) {
                p = S.get(i);
                min = dist;
                argmin = i;
            }
        }
        return new Tuple2(argmin, p);
    }

    public static ArrayList<Point> getRandomCenters(int dim, int k) {
        ArrayList<Point> S = new ArrayList<Point>(k);
        for (int i = 0; i <= k - 1; i++) {
            S.add(new PointCentroid(Utility.toArrayList(Evaluation.generateRandomVector(dim).toArray())));
        }
        return S;
    }

    public static Tuple2<Integer, Point> mostFar(ArrayList<Point> P, Point c) {
        Point p = null;
        double max = Double.MIN_VALUE;
        int argmax = -1;
        for (int i = 0; i < P.size(); i++) {
            double dist = Distance.calculateDistance(P.get(i).parseVector(), c.parseVector(), "standard");
            if (dist >= max && P.get(i).equals(p) == false) {
                p = P.get(i);
                max = dist;
                argmax = i;
            }
        }
        return new Tuple2(argmax, p);
    }

    public static Tuple2<Integer, Point> mostFar(ArrayList<Point> P, ArrayList<Point> S) {
        Point p = null;
        double max = Double.MIN_VALUE;
        int argmax = -1;
        for (int i = 0; i < S.size(); i++) {
            Tuple2<Integer, Point> t = Utility.mostClose(P, S.get(i));
            double dist = Distance.calculateDistance(t._2.parseVector(), S.get(i).parseVector(), "standard");
            if (dist >= max && P.get(i).equals(p) == false) {
                p = S.get(i);
                max = dist;
                argmax = i;
            }
        }
        return new Tuple2(argmax, p);
    }

    public static double[] toDoubleArray(ArrayList v) {
        // Una fantastica porcata
        double[] dd = new double[v.size()];
        for (int i = 0; i < v.size(); i++) {
            dd[i] = (Double) v.get(i);
        }
        return dd;
    }
    
  public static ArrayList<Vector>  PCA( List<Vector> rowsList,SparkContext sc) throws Exception
  {  
      return Utility.PCA(rowsList,sc,2,true);
  }    
  public static ArrayList<Vector>  PCA( List<Vector> rowsList,SparkContext sc,int numComp,boolean normalize) throws Exception
  {     
    if(normalize)
    {
        rowsList=Utility.normalize2(rowsList,sc);
    }
    JavaRDD<Vector> rows = JavaSparkContext.fromSparkContext(sc).parallelize(rowsList);
    // Create a RowMatrix from JavaRDD<Vector>.
    RowMatrix mat = new RowMatrix(rows.rdd());
    Matrix pc = mat.computePrincipalComponents(numComp); 
    RowMatrix projected = mat.multiply(pc);

        long numRows = projected.numRows();
        System.out.println(numRows);
        //dobbiamo ancora fare la normalizzazione :(
        DenseMatrix<Object> toBreeze = projected.toBreeze();
        ArrayList<Vector> vecs=new ArrayList<Vector>();
        for(int i=0;i<numRows;i++)
        {
            double d[]=new double[numComp];
            for(int j=0;j<numComp;j++)
            {
                d[j]=(Double)toBreeze.apply(i,j);
            }
            Vector v=Vectors.dense(d);
            vecs.add(v);
        }
    return vecs;
  }
  
  static JavaDoubleRDD removeOutliersMR(JavaDoubleRDD rdd) 
  {
    final StatCounter summaryStats = rdd.stats();
    final Double stddev = Math.sqrt(summaryStats.variance());
    return rdd.filter(new Function<Double, Boolean>() { public Boolean call(Double x) {
          return (Math.abs(x - summaryStats.mean()) < 3 * stddev);
        }});
  }
  
  
  public static List<Vector> normalize2(List<Vector> rowsList,SparkContext sc)
  {
      //non vedo questa grande differenza ma vabbene
      //non nella forma almeno
      //efficiente : questo
      //corretto : immagino l'altro ma se non va...
      //intanto proviamo questo ci volgiono 2 minuti
      for(int i=0;i<rowsList.size();i++)
      {
          //BLAS crea un ml.vector.....
          //<angry><angry><angry><angry><angry><angry><angry><angry><angry>
          //tra il dire e il fare c'è di mezzo spark
          //a posto
         BLAS.scal( Vectors.norm(rowsList.get(i), 2),  rowsList.get(i));
         rowsList.set(i,rowsList.get(i) );  
      }
      return rowsList;
      
  }
  public static List<Vector> normalize(List<Vector> rowsList,SparkContext sc)
  {
      //un copia incolla non hai mai ucciso nessuno
      //diamo a cesare quel che è di cesare
      //se lui vuole ml.vector noi gli diamo ml.vector
      int id=0;
      //lassa stare va....
      List<Row> data=new ArrayList<Row>();
      for(int i=0;i<rowsList.size();i++)
      {
        
        data.add( RowFactory.create(id, rowsList.get(i).asML()));
      }  
        StructType schema = new StructType(new StructField[]{
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(data, schema);

        // Normalize each Vector using $L^1$ norm.
        Normalizer normalizer = new Normalizer()
          .setInputCol("features")
          .setOutputCol("normFeatures")
          .setP(1.0);

        Dataset<Row> l1NormData = normalizer.transform(dataFrame);
        //l1NormData.show();

        // Normalize each Vector using $L^\infty$ norm.
        Dataset<Row> lInfNormData =
          normalizer.transform(dataFrame, normalizer.p().w(Double.POSITIVE_INFINITY));
        //lInfNormData.show(); 
        List<Row> normalized = lInfNormData.collectAsList();
        
      /*List<Vector> normalizedData=new ArrayList<Vector>();
      for(int i=0;i<normalized.size();i++)
      {
        normalizedData.add( (Vector)normalized.get(i).apply(1));
      } */        
      //return normalizedData;
      return null;
        
  }
  
  public static JavaRDD<Point> leggiInput(String s,JavaSparkContext sc)
  {
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
        return points;
  }
    public static ArrayList<Point> leggiInputLocale(String input) throws FileNotFoundException, IOException
  {
        File file=new File(input);
        //System.out.println(file.getAbsolutePath());
        FileReader fr = new FileReader(file);
        BufferedReader bf=new BufferedReader(fr);
        String s;
        ArrayList<Point> points=new ArrayList<Point> ();
        while((s=bf.readLine())!=null)
        {
            String[] split = s.split("   "); 
            //System.out.println(split.length);
            ArrayList<Double> al=new ArrayList<Double>();
            for(String ss:split)
            {
                if(ss.length()!=0)al.add(Double.parseDouble(ss));
            }
            //System.out.println(new PointCentroid(al));
            points.add(new PointCentroid(al));
            
        }
        return points;
  }
  public static void writeOuptut(String s,Clustering C) throws IOException
  {
        File file=new File(s);
        //System.out.println(file.getAbsolutePath());
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
  }
    
    
}
