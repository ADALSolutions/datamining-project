package it.unipd.dei.dm1617;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.mllib.linalg.Vector;

import scala.Tuple2;

public class ClusteringBuilderNew extends ClusteringBuilder {

	public ClusteringBuilderNew() {
		// TODO Auto-generated constructor stub
	}
	
	public static Clustering kmeansAlgorithm(ArrayList<Point> P, ArrayList<Point> S, int k) {
		// Clustering primeClustering = ClusteringBuilder.Partition(P, S, k);
		Clustering primeClustering = ClusteringBuilder.Partition_optimized(P, S, k, true, false, null);
		boolean stopping_condition = false;
		double phi = primeClustering.kmeans();
		boolean stoppingCondition = false;
		int cont = 0;
		while (!stoppingCondition) {
			// Calcolo nuovo Clustering
			ArrayList<Point> centroids = primeClustering.getCentroids();
			// Clustering secondClustering = ClusteringBuilder.Partition(P,
			// centroids, k);
			Clustering secondClustering = ClusteringBuilder.Partition_optimized(P, centroids, k, false, false,
					primeClustering);
			double phikmeans = secondClustering.kmeans();
			// Valuto se quello nuovo è megliore rispetto a quello vecchio
			if (phi > phikmeans) {
				phi = phikmeans;
				primeClustering = secondClustering;
			} else {
				stoppingCondition = true;
			}
			cont++;
		}
		// System.out.println("Cont KMeans: " + cont);
		// ClusteringBuilder.numIter = cont;
		contMeans = contMeans + cont;
		return primeClustering;
	}
	
	public static Clustering kmeansEuristic(ArrayList<Point> P, ArrayList<Point> S, int k) {
		// System.out.println("Inizio algoritmo");
		Clustering primeClustering = ClusteringBuilder.Partition_optimized(P, S, k, true, true, null);
		boolean stopping_condition = false;
		double phi = primeClustering.kmeans();
		boolean stoppingCondition = false;
		int cont = 0;

		while (!stoppingCondition) {
			cont++;

			// Calcolo nuovo Clustering
			ArrayList<Point> centroids = primeClustering.getCentroids();
			Clustering secondClustering = ClusteringBuilder.Partition_optimized(P, centroids, k, false, true,
					primeClustering);
			double phikmeans = secondClustering.kmeans();
			// Valuto se quello nuovo è megliore rispetto a quello vecchio

			// System.out.println(""+Math.abs(phi-phikmeans)+"
			// "+1/(double)cont);
			if (phi > phikmeans) // || Math.abs(phi-phikmeans) >1/(double)cont
			{
				phi = phikmeans;
				primeClustering = secondClustering;
			} else {
				stoppingCondition = true;
			}
			/*
			 * if (cont >= ClusteringBuilder.numIter / 2) { break; }
			 */
		}
		// System.out.println("Cont Euristico: " + cont);
		contEuristico = contEuristico + cont;
		return primeClustering;
	}

	
	
	
}
