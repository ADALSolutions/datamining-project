package GoalA.examples;

import java.util.Properties;

import GoalA.InputOutput;
import GoalA.WikiPage;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Extract a sample of pages from the given dataset, writing it in the
 * given datase.
 */
public class Sample {

  public static void main(String[] args) {
	 
	System.setProperty("hadoop.home.dir", "C:\\Users\\alvis\\Desktop\\Data Mining\\Progetto\\Project");
	
	String inputPath = "C:\\Users\\alvis\\Desktop\\Data Mining\\Progetto\\Dataset\\medium-sample.dat.bz2";
    String outputPath = "C:\\Users\\alvis\\Desktop\\Data Mining\\Progetto\\Dataset\\output-medium-sample";
    double fraction = 0.1;

    // The usual Spark setup
    SparkConf conf = new SparkConf(true).setAppName("Sampler").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Read the pages from the path provided as the first argument.
    JavaRDD<WikiPage> pages = InputOutput.read(sc, inputPath);

    // Sample, without replacement, the desired fraction of pages.
    JavaRDD<WikiPage> sample = pages.sample(false, fraction);

    // Write the sampled pages to the given output path.
    InputOutput.write(sample, outputPath);
  }

}
