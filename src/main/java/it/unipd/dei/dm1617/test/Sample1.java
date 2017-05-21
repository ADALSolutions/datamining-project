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
public class Sample1 {

  public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {

    System.out.println("Sample");
    System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Esempi\\dm1617-project-stub");
    SparkConf conf = new SparkConf(true).setAppName("Sampler");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //sc.sc().setLogLevel("ERROR");
    
    String inputPath = "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Esempi\\dm1617-project-stub\\medium-sample.dat.bz2";          //medium-sample.dat.bz2
    String outputPath="C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Esempi\\dm1617-project-stub\\output"; 
    String modelPath="C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Esempi\\dm1617-project-stub\\model\\Word2Vec"; 
    double fraction=0.1; 
    
    JavaRDD<WikiPage> pages = InputOutput.read(sc, inputPath);//insert inputPath instead outputPath if you want all dataset instead of sample
    //Trovare d-bound
    pages = pages.sample(false, fraction);   
    JavaRDD<String> texts = pages.map((p) -> p.getText());
    JavaRDD<Iterable<String>> lemmas = Lemmatizer.lemmatize(texts).cache();
    //Once time InputOutput.write(sample, outputPath);
      //Encoder<ArrayList> personEncoder = Encoders.bean( ArrayList.class);
      //Dataset<ArrayList> javaBeanDS = SparkSession.builder().getOrCreate().createDataset((ArrayList)lemmas.collect(), personEncoder);      
      //ONCE TIME, only first time when i fit the model after i load model
      /*Word2Vec word2vec = new Word2Vec();
      Word2VecModel fit = word2vec.fit(lemmas);
      fit.save(sc.sc(), modelPath);
      */
      Word2VecModel load = Word2VecModel.load(sc.sc(), modelPath);
          System.out.println("Modello caricato");
      Broadcast<Word2VecModel> b=sc.broadcast(load);
      Vector transform = load.transform(load.org$apache$spark$mllib$feature$Word2VecModel$$wordList()[0]);
      int size=transform.size();
      System.out.println("size:"+String.valueOf(size));
      JavaRDD<Vector> map;
      System.out.println("Inizio Map");
      map = lemmas.map(
              new Function<Iterable<String>, Vector>() {
        @Override
        public Vector call(Iterable<String> A) throws Exception {
            Vector sum = Vectors.zeros(size);
            System.out.println("pr");
            int cont=0;
            for(String s:A)
            {
                
                //Non serve più breeze.linalg.Vector<Object> asBreeze = sum.toDense().asBreeze();
                //double a,Vector x,Vector y)  y += a * x   
                BLAS.axpy(0, sum, load.transform(s));
                cont++;
            }
            BLAS.scal( ((double)1)/cont , sum);
            System.out.println(sum);
            return sum;
        }
    });
      JavaRDD<Vector> cache = map.cache();
      cache.collect();
      
    /*
    JavaRDD<Vector> tf = new CountVectorizer()
      .setVocabularySize(100)
      .transform(lemmas)
      .cache();
    */
    while(true){}
  }

}

