/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import Jama.EigenvalueDecomposition;
import breeze.linalg.DenseMatrix;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
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

    public static ArrayList<Point> initMedianCenters(ArrayList<Point> points,int k) 
    {
        ArrayList<Point> P=(ArrayList<Point>)points.clone();
        ArrayList<Cluster> clusters = new ArrayList<Cluster>();
        for (int i = 0; i < k; i++) {
            Cluster C = new Cluster();
            clusters.add(C);
        }
        Random r=new Random();
        int l=P.size()/k;
        for (int i = 0; i <k; i++) 
        {
            for(int j=0;j<l;j++)
            {
                int nextInt = r.nextInt(P.size());     
                Point p=P.get(nextInt);
                clusters.get(i).getPoints().add(p);
                P.remove(nextInt);
            }  
        }
        clusters.get(clusters.size()-1).getPoints().addAll(P);
        for (int i = 0; i < k; i++) {
            Cluster C=clusters.get(i);
            C.setCenter(C.calculateCentroid());
        } 
        ArrayList<Double> sums=new ArrayList<Double>();
        for (int i = 0; i <k; i++) 
        {
            Cluster C=clusters.get(i);
            double sum=0;
            for(int j=0;j<C.getPoints().size();j++)
            {
                double dist=Distance.calculateDistance( C.getPoints().get(j).parseVector(),C.getCenter().parseVector(),"standard");
                C.getPoints().get(j).setDist(dist);
                sum+=dist;
            }
            sums.add(sum);
        }
        ArrayList<Point> centers=new ArrayList<Point> ();
        for (int i = 0; i <k; i++) 
        {
            Cluster C=clusters.get(i);
            Object[] toArray = C.getPoints().toArray();
            Arrays.sort(toArray);
            double acc=0;
            double sum=sums.get(i)/2;
            for(int j=0;j<toArray.length;j++)
            {
                acc+=((Point)toArray[j]).getDist();
                if(acc>sum)
                {
                    double diff1=Math.abs(sum-(acc-((Point)toArray[j]).getDist()));
                    double diff2=Math.abs(acc-sum);
                    if(diff1<diff2)centers.add((Point)toArray[j-1]);
                    else centers.add((Point)toArray[j]);
                    break;
                }
            }
        }
        return centers;
       
    }
    public static List<Vector> toListVector(double[][] array )
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
            if (dist <= min && P.get(i).equals(c) == false) {
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
            if (dist <= min ) {//tolto && P.get(i).equals(p) == false
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
            if (dist >= max) {//tolto  && P.get(i).equals(p) == false
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

  public static Clustering  PCAClustering( Clustering C,JavaSparkContext sc,int numComp,boolean normalize) throws Exception
  {
        ArrayList<Point> P = C.getPoints();
        ArrayList<Point> centers = C.getCenters();
        //Creo vettori
        ArrayList<Vector> vectors = new ArrayList<Vector>();
        for (Point p : P) {
            Vector v = p.parseVector();
            vectors.add(v);
        }
        //Trasformo vettori
        ArrayList<Vector> PCAs = Utility.PCA(vectors, sc.sc(), numComp,false, true);
        //Creo punti
        ArrayList<Point> pointsPCA = new ArrayList<Point>();
        ArrayList<Point> centersPCA = new ArrayList<Point>();
        for (int i=0;i<PCAs.size();i++)//vectors
        {
            Point add=new PointCentroid(vectors.get(i));
            if(centers.contains(P.get(i)))
            {
                centersPCA.add(add);
            }
            pointsPCA.add(add);
        }
        
        ArrayList<Cluster> clusters = new ArrayList<Cluster>();
        HashMap<Point, Cluster> map = new HashMap<Point, Cluster>();

        for (int i = 0; i < C.getK(); i++) {
            Cluster CL = new Cluster();
            CL.setCenter(centers.get(i));
            clusters.add(CL);
        }
        for(int i=0;i<P.size();i++)
        {
            Cluster get = C.getMap().get(P.get(i));
            
        }
        return null;

    }
  
  public static ArrayList<Point>  reducePointsDim(ArrayList<Point> P,SparkContext sc,int numComp)
  {
        ArrayList<Vector> vectors = new ArrayList<Vector>();
        for (Point p : P) {
            Vector v = p.parseVector();
            vectors.add(v);
        } 
        ArrayList<Point> Pnew=new ArrayList<Point>();
        for (Vector v : vectors)//vectors
        {
            double d[]=new double[numComp];
            for(int i=0;i<numComp;i++)
            {
                d[i]=v.apply(i);
            }
            Vector dense = Vectors.dense(d);
            Pnew.add(new PointCentroid(dense));
        }
        return Pnew;
  }
  public static ArrayList<Point>  PCAPoints(ArrayList<Point> P,SparkContext sc,int numComp,boolean optimal,boolean normalize) throws Exception
  {  
        ArrayList<Vector> vectors = new ArrayList<Vector>();
        for (Point p : P) {
            Vector v = p.parseVector();
            vectors.add(v);
        }
        //Trasformo punti in vettori
        ArrayList<Vector> PCAs = Utility.PCA(vectors, sc,numComp,optimal,normalize);
        //Trasformo vettori in punti, lo uso solo se voglio stampare il clustering fatto in 2d
        ArrayList<Point> pointsPCA = new ArrayList<Point>();
        for (Vector v : PCAs)//vectors
        {
            pointsPCA.add(new PointCentroid(v));
        }
        return pointsPCA;
  }   
  public static ArrayList<Vector>  PCA( List<Vector> rowsList,SparkContext sc) throws Exception
  {  
      return Utility.PCA(rowsList,sc,2,false,true);
  } 
//trova il numero ottimale di componenti  
  public static int  optimalNumComp(RowMatrix mat ) throws Exception
  {  
      int numCols=(int) mat.numCols();
      int numRows = (int) mat.numRows();
      DenseMatrix<Object> toBreeze = mat.toBreeze();
      double d[][]=new double[numRows][numCols];
      for(int i=0;i<numRows;i++)
      {
          for(int j=0;j<numCols;j++)
          {
              d[i][j] =(Double) toBreeze.apply(i,j);
              
          }
      }
      Jama.Matrix M=new Jama.Matrix(d);
      EigenvalueDecomposition e = M.eig();
      double[] Eigenvalues = e.getRealEigenvalues(); 
      ArrayList<Double> AL=new ArrayList<Double>();
      double sum=0;
      for(int i=0;i<Eigenvalues.length;i++)
      {
          if(Eigenvalues[i]>0){AL.add(Eigenvalues[i]);sum+=Eigenvalues[i];}
      }
      double dd[]=new double[AL.size()];
      Object[] toArray = AL.toArray();
      for(int i=0;i<toArray.length;i++)
      {
          dd[i]=(Double)toArray[i];   
          //System.out.println(dd[i]/sum);
      }
      Arrays.sort(dd);
      double acc=0;
      int cont=0;    
      for(int i=0;i<dd.length;i++)
      {
            acc+=dd[i];
            if(acc/sum<0.95)
            {
                cont++;
            }
      }
      
     System.out.println("DimensioniIniziali: "+numCols+"  DimensioniFinali: "+cont);
     return cont; 
      
  }  
  //Data una lista di vettori restituisce i vettori dopo la PCA
  //Se optimal=true allora sceglie il valore ottimale
  //Se normalize è true allora fa la normalizzazione
  public static ArrayList<Vector>  PCA( List<Vector> rowsList,SparkContext sc,int numComp,boolean optimal,boolean normalize) throws Exception
  {     
    if(normalize)
    {
        rowsList=Utility.normalize2(rowsList,sc);
    }
    JavaRDD<Vector> rows = JavaSparkContext.fromSparkContext(sc).parallelize(rowsList);
    // Create a RowMatrix from JavaRDD<Vector>.
    RowMatrix mat = new RowMatrix(rows.rdd());
    if(optimal)numComp=Utility.optimalNumComp(mat );
    System.err.println("STOP");
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
      for(int i=0;i<rowsList.size();i++)
      {
         //System.out.println("PRIMA: "+rowsList.get(i));
         BLAS.scal( 1/Vectors.norm(rowsList.get(i), 2),  rowsList.get(i));
         rowsList.set(i,rowsList.get(i) );  
         //System.out.println("DOPO: "+rowsList.get(i));
      }
      return rowsList;
      
  }
  public static List<Vector> normalize(List<Vector> rowsList,SparkContext sc)
  {
      int id=0;
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
      JavaRDD<String> textFile = sc.textFile(s);
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
  
  public static ArrayList<Point> copy( ArrayList<Point> points)
  {
       ArrayList<Point> P=new  ArrayList<Point>();
       for(Point p:points)
       {
           P.add(p.copy());
       }
       return P;
  }
    
    
}
