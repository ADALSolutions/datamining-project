package it.unipd.dei.dm1617.test;

import it.unipd.dei.dm1617.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.broadcast.*;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.spark.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Example program to show the basic usage of some Spark utilities.
 */
public class TestOnWikiDataset {

	public static final int VSIZE = 100;
	public static double start;
	public static double end;

	public static int MAXK = 10;

	public static void main(String[] args) {
		String dataPath = args[0];
		String modelPath = args[1];

		// Usual setup
		SparkConf conf = new SparkConf(true).setAppName("TestOnWikiDataset");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load dataset of pages
		System.out.println("Reading dataset...");
		start = System.currentTimeMillis();
		JavaRDD<WikiPage> pages = InputOutput.read(sc, dataPath);
		end = System.currentTimeMillis();
		System.out.printf("Time to read the dataset = %s milliseconds%n", end - start);

		// Get text out of pages
		System.out.println("Extracting texts...");
		start = System.currentTimeMillis();
		JavaRDD<String> texts = pages.map((p) -> p.getText());
		end = System.currentTimeMillis();
		System.out.printf("Time to extract texts = %s milliseconds%n", end - start);

		// Get the lemmas. It's better to cache this RDD since the
		// following operation, lemmatization, will go through it two
		// times.
		System.out.println("Lammatizing texts...");
		start = System.currentTimeMillis();
		JavaRDD<Iterable<String>> lemmas = (JavaRDD<Iterable<String>>) Lemmatizer.lemmatize(texts).cache();
		end = System.currentTimeMillis();
		System.out.printf("Time to lemmatize texts = %s milliseconds%n", end - start);

		// Remove stop words
		System.out.println("Removing stop words...");
		start = System.currentTimeMillis();
		Broadcast<Set<String>> bStopWords = sc.broadcast(
				new HashSet<>(Arrays.asList(StopWordsRemover.loadDefaultStopWords("english"))));
		lemmas.map((ls)
				-> {
			ArrayList<String> filtered = new ArrayList<>();
			for (String l : ls) {
				if (!bStopWords.getValue().contains(l)) {
					filtered.add(l);
				}
			}
			return filtered;
		});
		end = System.currentTimeMillis();
		System.out.printf("Time to remove stop words = %s milliseconds%n", end - start);

		// Create and configure an object of type Word2Vec which is needed to fit our model.
		Word2Vec word2vec = new Word2Vec()
				.setVectorSize(VSIZE)
				.setMinCount(0);

		//load or create model;
		Word2VecModel model;
		File f = new File(modelPath);
		if (f.exists()) {
			//Load the model
			System.out.println("Loading model...");
			start = System.currentTimeMillis();
			model = Word2VecModel.load(sc.sc(), modelPath);
			end = System.currentTimeMillis();
			System.out.printf("Time to load the model = %s milliseconds%n", end - start);
		}
		else {
			// Use the object of type Word2Vec to fit a model based on the preprocesed lemmatized dataset (lemmas).
			System.out.println("Fitting model...");
			start = System.currentTimeMillis();
			model = word2vec.fit(lemmas);
			end = System.currentTimeMillis();
			System.out.printf("Time to fit the model = %s milliseconds%n", end - start);

			// Save the model
			System.out.println("Saving model...");
			start = System.currentTimeMillis();
			model.save(sc.sc(), modelPath);
			end = System.currentTimeMillis();
			System.out.printf("Time to save the model = %s milliseconds%n", end - start);
		}

		// Transform the lemmas using the word2vec model.
		start = System.currentTimeMillis();
		System.out.println("Transforming lemmas into vectors representing lemmas (vector size = " + VSIZE + ")...");
		/*JavaRDD<Iterable<Vector>> vectorLemmas = lemmas.map((Iterable<String> a) -> {
			ArrayList<Vector> v = new ArrayList();
			for (String l : a) {
				v.add(model.transform(l));
			}
			return v;
		});*/
		JavaRDD<Vector> vectorDocs = lemmas.map((a)
				-> {
			int count = 0;
			Vector sum = Vectors.zeros(VSIZE);
			for (String l : a) {
				count++;
				Vector x = model.transform(l);
				BLAS.axpy(1, x, sum);
			}
			BLAS.scal(((double) 1) / count, sum);
			return sum;
		});
		end = System.currentTimeMillis();
		System.out.printf("Time to transform lemmas = %s milliseconds%n", end - start);

		//printrdd(vectorLemmas);

		//cache vector docs because kmeans is iterative
		System.out.println("caching vectors representing documents");
		vectorDocs.cache();
		
		/*
		//Set up experiments for increasing k values, calculate performance and evaluate Objective funcion.
		//Create a KMeans object.
		System.out.println("Initializing kmeans...");
		double cost;
		double time;
		double performance;
		System.out.printf("|    k    |   cost   |   time   | performance |");
		for (int k = 2; k < MAXK; k++) {
			start = System.currentTimeMillis();
			KMeansModel clustering = KMeans.train(vectorDocs.rdd(), k, 10);
			end = System.currentTimeMillis();
			/**
			 * Note that the fact that we can compute the cost of the clustering
			 * for a specific set of points means that we can train the
			 * clustering with a subset of points (for improved performances)
			 * and than evaluate the clustering on all points. For example we
			 * can train the clustering with the small dataset and evaluate it
			 * with the medium dataset, if we import it.
			 */
		/*
			cost = clustering.computeCost(vectorDocs.rdd());
			time = end - start;
			performance = ((double) 1) / (cost * time);
			System.out.printf("|   %s   |   %s   |   %s   |   %s   |", k, cost, time, performance);
		}
		*/
		List<Vector> vectors = vectorDocs.collect();
		try{
		PrintWriter writer = new PrintWriter("vector-docs-large-dataset.txt", "UTF-8");
		vectors.forEach((p) -> {
		writer.println(p.apply(0) + " " + p.apply(1));
		});
		writer.close();
		} catch (IOException e) {
		System.err.println(e);
		}
		
    

	}

	private static void printrdd(JavaRDD<Iterable<Vector>> rdd) {
		for (Iterable<Vector> i : rdd.take(10)) {
			for (Vector v : i) {
				System.out.println(v.toJson());
			}
		}
	}
	
}
