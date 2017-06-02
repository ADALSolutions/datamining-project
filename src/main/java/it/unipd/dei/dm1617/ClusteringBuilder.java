/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import java.util.ArrayList;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.json4s.Merge;
import scala.Tuple2;
import it.unipd.dei.dm1617.Cluster;

/**
 *
 * @author DavideDP
 */
public class ClusteringBuilder implements Serializable {

	public static int numIter = -1;

	public static Clustering Partition(ArrayList<Point> P, ArrayList<Point> S, int k) {
		if ((S.size() != k)) {
			throw new IllegalArgumentException("S must have size k ");
		}

		ArrayList<Cluster> clusters = new ArrayList<Cluster>();
		HashMap<Point, Cluster> map = new HashMap<Point, Cluster>();

		Clustering.setCenters(clusters, S);

		if (k == 1) {
			clusters.get(0).getPoints().addAll(P);
			return new Clustering(P, clusters, S);
		}

		Clustering.assignClusters(clusters, S, P, map);

		return new Clustering(P, clusters, S);// S centroidi
	}

	public static Clustering Partition_optimized(ArrayList<Point> P, ArrayList<Point> S, int k, boolean first,
			boolean euristic, Clustering C2) {
		if (first) {
			if ((S.size() == k)) {
			} else {
				throw new IllegalArgumentException("S must have size k ");
			}

			long start = System.currentTimeMillis();
			ArrayList<Cluster> clusters = new ArrayList<Cluster>();
			HashMap<Point, Cluster> map = new HashMap<Point, Cluster>();
			for (int i = 0; i < k; i++) {
				Cluster C = new Cluster();
				C.setCenter(S.get(i));
				clusters.add(C);
			}
			if (k == 1) {
				clusters.get(0).getPoints().addAll(P);
				return new Clustering(P, clusters, S);
			}
			for (Point p : P) {
				Vector parseVector = p.parseVector();
				Tuple2<Integer, Point> mostClose = Utility.mostClose(S, p);
				Cluster C = clusters.get(mostClose._1);
				map.put(p, C);
				C.getPoints().add(p);
				p.setDist(Distance.calculateDistance(p.parseVector(), C.getCenter().parseVector(), "standard"));

			}
			long end = System.currentTimeMillis();
			// System.out.println(","+(end-start));
			// Tempo Partition Inizial
			return new Clustering(P, clusters, S);// S centroidi
		} else {
			// Aggiornamento Clustering
			int cont2 = 0;

			for (Point p : P) {

				Vector parseVector = p.parseVector();
				Cluster C = C2.getMap().get(p);
				int index = C2.getClusters().indexOf(C);
				Point center = S.get(index);
				double dist2 = Distance.calculateDistance(parseVector, center.parseVector(), "standard");

				if (euristic) {

					if (p.getDist() > dist2) {

						long start = System.nanoTime();
						cont2++;
						// System.out.println("ENTRO");
						p.setDist(dist2);
						long end = System.nanoTime();
						// System.out.println("PezzoCorto: "+(end-start));
					} else {
						long start = System.nanoTime();
						ArrayList<Cluster> clusters = C2.getClusters();
						// ArrayList<Point> centers = C2.getCenters();
						Tuple2<Integer, Point> mostClose = Utility.mostClose(S, p);
						C = clusters.get(mostClose._1);
						C2.assignPoint(p, C);
						p.setDist(Distance.calculateDistance(p.parseVector(), mostClose._2.parseVector(), "standard"));
						long end = System.nanoTime();
						// System.out.println("PezzoLungo: "+(end-start));
					}
				} else {
					ArrayList<Cluster> clusters = C2.getClusters();
					// ArrayList<Point> centers = C2.getCenters();
					Tuple2<Integer, Point> mostClose = Utility.mostClose(S, p);
					C = clusters.get(mostClose._1);
					C2.assignPoint(p, C);
					p.setDist(Distance.calculateDistance(p.parseVector(), mostClose._2.parseVector(), "standard"));
				}

			}
			// System.out.println("Risparmio : "+cont2);
			for (int i = 0; i < S.size(); i++) {
				C2.getClusters().get(i).setCenter(S.get(i));
			}
			return C2;

		}
	}

	public static Clustering FarthestFirstTraversal(ArrayList<Point> P, int k) {

		/*
		 * rimuovo i punti dal clustering in modo da tener traccia dei rimanenti
		 * P\cc poi li riaggiungo tutti alla fine, credo sia più efficiente
		 */
		Point first = P.get(new Random().nextInt(P.size()));
		ArrayList<Point> S = new ArrayList<Point>();
		S.add(first);
		P.remove(first);
		// se p.size<k allora raise error
		for (int i = 0; i <= k - 2; i++) {
			// prendo come inizio sempre la distanza tra il primo
			// centroide(first) e il primo punto(P.get(0))
			double max = Distance.calculateDistance(first.parseVector(), P.get(0).parseVector(), "standard");
			int argmax = 0;
			for (int j = 0; j < P.size(); j++) {
				Point esamina = P.get(j);
				for (int l = 0; l < S.size(); l++) {
					double dist = Distance.calculateDistance(esamina.parseVector(), S.get(l).parseVector(), "standard");
					if (dist > max) {
						max = dist;
						argmax = j;
					}
				}
			}
			S.add(P.get(argmax));
			P.remove(argmax);
		}
		P.addAll(S);
		return ClusteringBuilder.Partition(P, S, k);
	}

	public static Clustering kmeansAlgorithm_old(ArrayList<Point> P, ArrayList<Point> S, int k) {
		Clustering primeClustering = ClusteringBuilder.Partition_optimized(P, S, k, true, false, null);
		boolean stopping_condition = false;
		double phi = primeClustering.kmeans();
		boolean stoppingCondition = false;
		int cont = 0;
		while (!stoppingCondition) {
			// Calcolo nuovo Clustering
			ArrayList<Point> centroids = primeClustering.getCentroids();
			Clustering secondClustering = ClusteringBuilder.Partition_optimized(P, centroids, k, true, false,
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
		// System.out.println("Counting KMeans: " + cont);
		return primeClustering;
	}

	public static Clustering kmeansEuristic_old(ArrayList<Point> P, ArrayList<Point> S, int k) {
		Clustering primeClustering = ClusteringBuilder.PartitionEuristic_old(P, S, k, true, null);
		;
		boolean stopping_condition = false;
		double phi = primeClustering.kmeans();
		boolean stoppingCondition = false;
		int cont = 0;
		while (!stoppingCondition) {
			cont++;
			// Calcolo nuovo Clustering
			ArrayList<Point> centroids = primeClustering.getCentroids();
			Clustering secondClustering = ClusteringBuilder.PartitionEuristic_old(P, centroids, k, false,
					primeClustering);
			double phikmeans = secondClustering.kmeans();
			// Valuto se quello nuovo è megliore rispetto a quello vecchio
			if (phi > phikmeans) {
				phi = phikmeans;
				primeClustering = secondClustering;
			} else {
				stoppingCondition = true;
			}
		}
		System.out.println("Cont: " + cont);
		return primeClustering;
	}

	public static ArrayList<Point> getRandomCenters(ArrayList<Point> P, int k) {
		ArrayList<Point> S = new ArrayList<Point>(k);
		Random r = new Random();
		for (int i = 0; i <= k - 1; i++) {
			int index = r.nextInt(P.size());
			S.add(P.get(index));
		}
		return S;
	}

	public static ArrayList<Point> kmeansPlusPlus(ArrayList<Point> P, int k) {
		Point c1 = ClusteringBuilder.getRandomCenters(P, 1).get(0);
		ArrayList<Point> S = new ArrayList<Point>();
		S.add(c1);
		P.remove(c1);
		ArrayList<Double> dP = new ArrayList<Double>(P.size());
		for (int i = 2; i < k; i++) {
			double sum = 0;
			// Calcolare dP
			for (int j = 0; j < P.size(); j++) {
				double min = Distance.calculateDistance(P.get(j).parseVector(), S.get(0).parseVector(), "standard");
				for (int l = 0; l < S.size(); l++) {
					double dist = min = Distance.calculateDistance(P.get(j).parseVector(), S.get(l).parseVector(),
							"standard");
					if (dist < min) {
						min = dist;
					}
				}
				dP.add(j, min);
				sum += Math.pow(min, 2);
			}
			// Calcolo Dp/sum
			for (int j = 0; j < P.size(); j++) {
				dP.set(j, Math.pow(dP.get(j), 2) / sum);
			}
			// Scelgo a random un punto c_i
			double threshold = Math.random();
			double sum2 = 0;
			// continuo a iterare finchè la somma non supera la threshold
			// quando succede ho beccato il c_i che cerco
			for (int j = 0; j < P.size(); j++) {
				sum2 += dP.get(j);
				if (sum2 >= threshold) {
					S.add(P.get(j));
					P.remove(P.get(j));
					break;
				}
			}
		}
		return S;
	}

	public static Clustering kmeansPlusPlusAlgorithm(ArrayList<Point> P, int k) {
		ArrayList<Point> S = ClusteringBuilder.kmeansPlusPlus(P, k);
		return ClusteringBuilder.kmeansAlgorithm_old(P, S, k);
	}

	public static Clustering PartitioningAroundMedoids(ArrayList<Point> P, ArrayList<Point> S, int k) {
		Clustering C_init = ClusteringBuilder.Partition(P, S, k);
		boolean stopping_condition = false;
		while (!stopping_condition) {
			stopping_condition = true;
			P.removeAll(S);
			boolean condition_exit = false;
			for (int i = 0; i < P.size(); i++) {
				if (condition_exit) {
					break;
				}
				Point p = P.get(i);
				for (int j = 0; j < S.size(); j++) {
					P.removeAll(S);// usare array per fare queste rimozioni è
									// pesantissimo
					ArrayList<Point> Sprimo = (ArrayList<Point>) S.clone();
					// aggiungo tutti i punti in P per fare il PARTITION
					P.addAll(S);// contiene anche c e p
					Point c = S.get(j);
					Sprimo.remove(c);
					Sprimo.add(p);
					// DA QUI IN POI P HA TUTTI I PUNTI
					Clustering Cprimo = ClusteringBuilder.Partition(P, Sprimo, k);
					double phimedianPrimo = Cprimo.objectiveFunction("kmedian");
					double phimedian = C_init.objectiveFunction("kmedian");
					if (phimedianPrimo < phimedian) {
						stopping_condition = false;
						C_init = Cprimo;
						condition_exit = true;
						S = Sprimo;
						break;
					}

				}
			}
		}
		return C_init;
	}

	public static Clustering hierarchicalClustering(ArrayList<Point> P, int k,
			BiFunction<Clustering, Clustering, Boolean> f) {
		Clustering C = ClusteringBuilder.Partition(P, ClusteringBuilder.getRandomCenters(P, k), k);
		ArrayList<Cluster> al = new ArrayList<Cluster>();
		for (Point p : P) {
			ArrayList<Point> alp = new ArrayList<Point>();
			alp.add(p);
			Cluster cluster = new Cluster(alp);
		}
		MergingCriterion mc = new MergingCriterion(C);
		while (!f.apply(C, null)) {
			mc.mergeSingleLinkage();
		}
		return C;
	}

	public static Clustering clusteringAlgorithm(ArrayList<Point> P, int k,
			BiFunction<Clustering, Clustering, Boolean> f, String algorithm) {
		if (algorithm.equals("kcenter")) {
			return FarthestFirstTraversal(P, k);
		} else if (algorithm.equals("kmeans")) {
			ArrayList<Point> S = ClusteringBuilder.getRandomCenters(P, k);
			return kmeansAlgorithm_old(P, S, k);
		} else if (algorithm.equals("kmeansPlusPlus")) {
			return kmeansPlusPlusAlgorithm(P, k);
		} else if (algorithm.equals("kmedian")) {
			ArrayList<Point> S = ClusteringBuilder.getRandomCenters(P, k);
			return PartitioningAroundMedoids(P, S, k);
		} else if (algorithm.equals("hierarchical") && f != null) {
			return hierarchicalClustering(P, k, f);
		}
		return null;
	}

	public static Clustering bestkClustering(ArrayList<Point> P, String algorithm,
			BiFunction<Clustering, Clustering, Boolean> f) {
		int k = 1;
		Clustering C = ClusteringBuilder.clusteringAlgorithm(P, k, f, algorithm);
		double phikprime = C.objectiveFunction(algorithm);
		// algorithm == objective function
		k = 2;
		Clustering C2 = ClusteringBuilder.clusteringAlgorithm(P, k, f, algorithm);
		while (f.apply(C, C2)) {
			C = C2;
			k = k * 2;
			phikprime = C.objectiveFunction(algorithm);
			C2 = ClusteringBuilder.clusteringAlgorithm(P, k, f, algorithm);
		}
		// search refinement
		int kprime = k / 2;
		return bisection(C, C2, kprime, k, phikprime, C2.objectiveFunction(algorithm), algorithm, f);

	}

	public static Clustering bisection(Clustering C, Clustering C2, int kprime, int k, double phikprime, double phik,
			String algorithm, BiFunction<Clustering, Clustering, Boolean> f) {
		int kmedio = (int) (kprime + k) / 2;
		if (kmedio == kprime) {
			return C2;
		}
		Clustering Cmedio = ClusteringBuilder.clusteringAlgorithm(C.getPoints(), kmedio, f, algorithm);
		double phikmedio = Cmedio.objectiveFunction(algorithm);

		if (f.apply(Cmedio, C2)) {
			return bisection(Cmedio, C2, kmedio, k, phikmedio, phik, algorithm, f);
		} else {
			return bisection(C, Cmedio, kprime, kmedio, phikprime, phikmedio, algorithm, f);
		}
	}

	public static Clustering PartitionEuristic_old(ArrayList<Point> P, ArrayList<Point> S, int k, boolean first,
			Clustering C2) {
		if (first) {
			if ((S.size() == k)) {
			} else {
				throw new IllegalArgumentException("S must have size k ");
			}

			ArrayList<Cluster> clusters = new ArrayList<Cluster>();
			HashMap<Point, Cluster> map = new HashMap<Point, Cluster>();
			for (int i = 0; i < k; i++) {
				Cluster C = new Cluster();
				C.setCenter(S.get(i));
				clusters.add(C);
			}
			if (k == 1) {
				clusters.get(0).getPoints().addAll(P);
				return new Clustering(P, clusters, S);
			}
			for (Point p : P) {
				Vector parseVector = p.parseVector();
				Tuple2<Integer, Point> mostClose = Utility.mostClose(S, p);
				Cluster C = clusters.get(mostClose._1);
				map.put(p, C);
				C.getPoints().add(p);
				p.setDist(Distance.calculateDistance(p.parseVector(), C.getCenter().parseVector(), "standard"));

			}
			return new Clustering(P, clusters, S);// S centroidi
		} else {
			if ((S.size() == k)) {
			} else {
				throw new IllegalArgumentException("S must have size k ");
			}

			ArrayList<Cluster> clusters = new ArrayList<Cluster>();
			HashMap<Point, Cluster> map = new HashMap<Point, Cluster>();
			// for i <- 1 to k do C_i <- null
			for (int i = 0; i < k; i++) {
				Cluster C = new Cluster();
				C.setCenter(S.get(i));
				clusters.add(C);
			}
			if (k == 1) {
				clusters.get(0).getPoints().addAll(P);
				return new Clustering(P, clusters, S);
			}
			for (Point p : P) {

				Vector parseVector = p.parseVector();
				Point center = C2.getMap().get(p).getCenter();
				int index = C2.getCenters().indexOf(center);
				Point centroid = S.get(index);
				double dist2 = Distance.calculateDistance(parseVector, centroid.parseVector(), "standard");
				if (p.getDist() == dist2) {// System.out.println("BAKA");
				}
				if (p.getDist() > dist2) {
					// System.out.println("ENTRO");
					p.setDist(dist2);
					Cluster CL = clusters.get(index);
					CL.getPoints().add(p);
					map.put(p, CL);

				} else {
					Tuple2<Integer, Point> mostClose = Utility.mostClose(S, p);
					Cluster C = clusters.get(mostClose._1);
					map.put(p, C);
					C.getPoints().add(p);
					p.setDist(Distance.calculateDistance(p.parseVector(), C.getCenter().parseVector(), "standard"));
				}

			}
			return new Clustering(P, clusters, S);

		}
	}

	public static int contEuristico;
	public static int contMeans;

}
