package it.unipd.dei.dm1617.test;

import it.unipd.dei.dm1617.InputOutput;
import it.unipd.dei.dm1617.WikiPage;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import it.unipd.dei.dm1617.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import breeze.linalg.SparseVector$;
import org.apache.spark.mllib.linalg.BLAS;


//import org.apache.spark.sql.DataFrame;
/**
 * Extract a sample of pages from the given dataset, writing it in the
 * given datase.
 */
public class SampleClient1 {

  public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
    System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Esempi\\dm1617-project-stub");
    SparkConf conf = new SparkConf(true).setAppName("Sampler");
    JavaSparkContext sc = new JavaSparkContext(conf);
String modelPath="C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Esempi\\dm1617-project-stub\\model\\Word2Vec";
Word2VecModel load = Word2VecModel.load(sc.sc(), modelPath);
System.out.println("Modello caricato");
      String[] op = load.org$apache$spark$mllib$feature$Word2VecModel$$wordList();
      for(String s:op)
      {
          System.out.println(s+" : "+load.transform(s).toString());
      }

    while(true){}
  }

}

