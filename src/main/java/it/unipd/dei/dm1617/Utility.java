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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
    public static Tuple2<Integer, Point> mostClose(Collection<Point> P, Point c) {
        Point p = null;
        double min = Double.MAX_VALUE;
        int argmin = -1;
        int i=0;
        for (Point ex:P) {
            double dist = Distance.calculateDistance(ex.parseVector(), c.parseVector());
            if (dist <= min && ex.equals(c) == false) {
                p = ex;
                min = dist;
                argmin = i;
            }
            i++;
        }
        return new Tuple2(argmin, p);
    }

    public static Tuple2<Integer, Point> mostClose(ArrayList<Point> P, ArrayList<Point> S) {
        Point p = null;
        double min = Double.MAX_VALUE;
        int argmin = -1;
        for (int i = 0; i < S.size(); i++) {
            Tuple2<Integer, Point> t = Utility.mostClose(P, S.get(i));
            double dist = Distance.calculateDistance(t._2.parseVector(), S.get(i).parseVector());
            if (dist <= min ) {//tolto && P.get(i).equals(p) == false
                p = S.get(i);
                min = dist;
                argmin = i;
            }
        }
        return new Tuple2(argmin, p);
    }


    public static Tuple2<Integer, Point> mostFar(ArrayList<Point> P, Point c) {
        Point p = null;
        double max = Double.MIN_VALUE;
        int argmax = -1;
        for (int i = 0; i < P.size(); i++) {
            double dist = Distance.calculateDistance(P.get(i).parseVector(), c.parseVector());
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
            double dist = Distance.calculateDistance(t._2.parseVector(), S.get(i).parseVector());
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
            return new PointSpark(al);        
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
            points.add(new PointSpark(al));
            
        }
        return points;
  }
  public static void writeOuptut2(String s,Clustering C) throws IOException
  {
        File file=new File(s);
        //System.out.println(file.getAbsolutePath());
        FileWriter fw = new FileWriter(file);
        for(Point p:C.getPoints())
        {
                Vector parse = p.parseVector();
                String stam=String.valueOf(parse.apply(0))+" "+String.valueOf(parse.apply(1));
                stam=stam+" "+C.getMap().get(p).toString();
                fw.write(stam+"\n");
                fw.flush();
        }
        fw.close();
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
 
  
  public static Tuple2<Double,Double> varianceMedia(List<Point> points)
  {
            double sum=0;
            double sum2=0;
          for(int j=0;j<points.size();j++)
          {
              sum+=points.get(j).getDist();
              sum2+=Math.pow(points.get(j).getDist(),2);
              
          }
          sum=sum/points.size();
          sum2=sum2/points.size();
          double var=sum2-Math.pow(sum,2);
          return new Tuple2(sum,var);
      
  }

    public static Vector generateRandomVector(int size) {
        double[] dd = new double[size];
        Random r = new Random();
        for (int i = 0; i < size; i++) {
            dd[i] = r.nextDouble() * Double.MAX_VALUE;
        }
        return Vectors.dense(dd);
    }
    
    
}
