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

/**
 *
 * @author DavideDP
 */
public class ClusteringBuilder implements Serializable {
	
	public static Clustering Partition(ArrayList<Point> P, ArrayList<Point> S,int k )
    {
    if ( (S.size()==k )) {}
    else{
      throw new IllegalArgumentException("S must have size k ");
    }  
    
    ArrayList<Cluster> clusters =new ArrayList<Cluster> (); 
    HashMap<Point,Cluster> map=new HashMap<Point,Cluster> ();
    	// for i <- 1 to k do C_i <- null
    	for(int i = 0; i < k; i++){
    		Cluster C = new Cluster();
    		C.setCenter(S.get(i));
    		clusters.add(C);
    	}
        if(k==1){clusters.get(0).getPoints().addAll(P);return new Clustering(P,clusters,S);}	
    	for(Point p: P){    
                Vector parseVector = p.parseVector();
    		// argmin computation
                //trovare un modo per applicare tutte le distanze in automatico,tipo un lemma tra i parametri
    		double min = Distance.calculateDistance(parseVector,S.get(0).parseVector(), "standard");
                int argmin=0;
    		for(int i = 1; i <= k - 1; i++){    
    			// Assuming Points = Vectors
    			double dist = Distance.calculateDistance(parseVector,S.get(i).parseVector(), "standard");		
    			if(dist < min){    				
    				min = dist;
    				argmin = i;    				
    			}   			
    		}
                Cluster C=clusters.get(argmin);
                map.put(p, C);
                C.getPoints().add(p);
                
    	}
        return new Clustering(P,clusters,S);//S centroidi        
    }
    
    public static Clustering FarthestFirstTraversal(ArrayList<Point> P, int k) {
            /*Clustering clustering=new Clustering( P,k);
	    Point first = clustering.getRandom();
            ArrayList<Point> S=clustering.getCentroids();
	    clustering.addCentroid(first);
	    clustering.removePoint(first);
             */

            // rimuovo i punti dal clustering in modo da tener traccia dei rimanenti P\cc
	    // poi li riaggiungo tutti alla fine, credo sia più efficiente
	    Point first = P.get(new Random().nextInt(P.size()));
        ArrayList<Point> S = new  ArrayList<Point>();
        S.add(first);
        P.remove(first);
	    //se p.size<k allora raise error 
	    for(int i = 0; i <= k - 2; i++) {
    		//prendo come inizio sempre la distanza tra il primo centroide e il primo punto
    		double max = Distance.calculateDistance(first.parseVector(), P.get(0).parseVector(), "standard");
            int argmax = 0;		
    		for(int j = 0; j <= P.size() - 1; j++) {
    			Point esamina = P.get(j);
    			for(int l = 0; l < S.size(); l++) {
    				double dist = Distance.calculateDistance(esamina.parseVector(), S.get(l).parseVector(), "standard");		
    				if(dist > max){    				
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
    
    public static Clustering kmeansAlgorithm(ArrayList<Point> P,ArrayList<Point> S,int k)
    {
        Clustering primeClustering=new Clustering(P);
        boolean stopping_condition=false;
        double phi = Double.MAX_VALUE;
        boolean stoppingCondition = false;
        
        while (!stoppingCondition)
        {
            primeClustering = ClusteringBuilder.Partition(P, S, k);
            primeClustering.getCenters();//Aggiorna i centroidi
            
            double phikmeans=primeClustering.kmeans();
            if( phi> phikmeans)
            {
                phi=phikmeans;
            }
            else
            {
                stoppingCondition = true;
            }
        }
        return primeClustering;
        
    }
    
    //scrive nuovi centroidi dentro al clustering
    public static void initRandomCentroids(Clustering primeClustering, int k) {
        for(int i = 0; i <= k - 1; i++) {
                Point centroid = primeClustering.getRandom();
                primeClustering.getCenters().add(centroid);
        }        
    }
    
    public static ArrayList<Point> initRandomCentroids(ArrayList<Point> P,int k)
    {
        ArrayList<Point> S=new ArrayList<Point>();
        Random r=new Random();
        for(int i = 0; i <= k - 1; i++)
        {
                int index=r.nextInt(P.size());
                S.add(P.get(index));
                
        }   
        return S;
    } 
    
    public static ArrayList<Point> kmeansPlusPlus(Clustering primeClustering, int k) {
        Point c1 = primeClustering.getRandom();
        ArrayList<Point> S = new ArrayList<Point>();
        ArrayList<Point> P = primeClustering.getPoints();
        S.add(c1);
        P.remove(c1);
        ArrayList<Double> dP = new ArrayList<Double>(P.size());
        for(int i = 2; i < k; i++) {
            double sum = 0;
            for(int j = 0; j < P.size(); j++) {
                double min = Distance.calculateDistance(P.get(j).parseVector(), S.get(0).parseVector(), "standard");
                for(int l = 1; l < S.size(); l++) {
                    double dist = min = Distance.calculateDistance(P.get(j).parseVector(), S.get(l).parseVector(), "standard");
                    if(dist < min) {    				
    				min = dist; 				
                    }  
                }
                dP.add(j, min);
                sum += Math.pow(min, 2);
            }
            
            for(int j = 0; j < P.size(); j++) {
                dP.set(j, Math.pow(dP.get(j), 2) / sum);
            }
            double threshold = Math.random();
            double sum2 = 0;
            for(int j = 0; j < P.size(); j++) {
                if(sum2 > threshold) {
                    S.add(P.get(j));
                    P.remove(P.get(j));
                    break;
                }
                else {
                    sum2 += dP.get(j);
                }
            }            
        }
        return primeClustering.getCenters();
    }
    
    public static Clustering kmeansPlusPlusAlgorithm(Clustering primeClustering, int k)
    {
    	ClusteringBuilder.kmeansPlusPlus(primeClustering,k);
    	return ClusteringBuilder.kmeansAlgorithm(primeClustering.getPoints(), primeClustering.getCenters(), k);
    }
    
    public static Clustering PartitioningAroundMedoids(ArrayList<Point> P, ArrayList<Point> S, int k) {
        Clustering C_init = ClusteringBuilder.Partition(P, S, k);
        boolean stopping_condition = false;       
        while (!stopping_condition) {
            stopping_condition = true;
            P.removeAll(S);
            boolean condition_exit = false;
            for (int i = 0; i < P.size(); i++) {
                if(condition_exit){
                	break;
                }
                Point p = P.get(i);
                ArrayList<Point> Sprimo = (ArrayList<Point>) S.clone();
                for(int j = 0; j < S.size(); j++)  {
                    Point c = S.get(j);
                    Sprimo.remove(c);
                    Sprimo.add(p);
                    P.addAll(S);
                    Clustering Cprimo = ClusteringBuilder.Partition(P, Sprimo, k);
                    double phimedianPrimo = Cprimo.objectiveFunction("kmedian");
                    double phimedian = C_init.objectiveFunction("kmedian");
                    if (phimedianPrimo < phimedian) {
                        stopping_condition = false;
                        C_init = Cprimo;
                        condition_exit = true;
                        break;
                    }
                }
            }
        }
        return C_init;        
    }      
        
    public static double[] toDoubleArray(ArrayList v) { // Una fantastica porcata
        double[] dd = new double[v.size()];
        for(int i = 0; i < v.size(); i++) {
           dd[i] =(Double)v.get(i);
        }
        return dd;
    }
    
    public static ArrayList toArrayList(double[]  dd) {
        java.util.ArrayList v = new java.util.ArrayList(dd.length);
        for(int i = 0; i < dd.length; i++) {
           v.add(dd[i]);
        }
        return v;       
    }
    
    public static Clustering hierarchicalClustering(ArrayList<Point> P, int k, BiFunction<Clustering,Clustering, Boolean> f) {
        Clustering C = new Clustering(P);
        //Creo N Clusters
        ArrayList<Cluster> al = new ArrayList<Cluster>();
        for(Point p : P) {
            ArrayList<Point> alp = new ArrayList<Point>();
            alp.add(p);
            Cluster cluster = new Cluster(alp);
        }
        MergingCriterion mc = new MergingCriterion(C);
        while(!f.apply(C, null)) { //ops
            mc.mergeSingleLinkage();
        }
        return C;
    }
    
    public static Clustering clusteringAlgorithm(ArrayList<Point> P, int k, BiFunction<Clustering,Clustering, Boolean> f, String algorithm) {
    	if (algorithm.equals("kcenter")){
    		return FarthestFirstTraversal(P,k);
    	}
    	else if (algorithm.equals("kmeans")){
    		ArrayList<Point> S = ClusteringBuilder.initRandomCentroids(P, k);
    		return kmeansAlgorithm(P, S, k);
    	}
    	else if (algorithm.equals("kmeansPlusPlus")){
    		Clustering C = new Clustering(P);
    		return kmeansPlusPlusAlgorithm(C, k);
    	}
    	else if (algorithm.equals("kmedian")){
    		ArrayList<Point> S = ClusteringBuilder.initRandomCentroids(P, k);
    		return PartitioningAroundMedoids(P, S, k);
    	}
    	else if (algorithm.equals("hierarchical") && f != null){
    		return hierarchicalClustering(P, k, f);
    	}
    	return null;
    }
    
    public static Clustering bestkClustering(ArrayList<Point> P, String algorithm, BiFunction<Clustering,Clustering, Boolean> f) {
    	int k=1;
    	Clustering C = ClusteringBuilder.clusteringAlgorithm(P, k, f, algorithm);
    	double phikprime=C.objectiveFunction(algorithm);
    	//algorithm == objective function
    	k=2;
    	Clustering C2 = ClusteringBuilder.clusteringAlgorithm(P, k, f, algorithm);
    	while (f.apply(C, C2)) {
    		C = C2;
    		k=k*2;
    		phikprime=C.objectiveFunction(algorithm);
    		C2 = ClusteringBuilder.clusteringAlgorithm(P, k, f, algorithm);
    	}   
    	// search refinement
    	int kprime = k/2; 
    	return bisection(C, C2, kprime,k,phikprime,C2.objectiveFunction(algorithm),algorithm,f);
    	
    }
    public static Clustering bisection(Clustering C, Clustering C2, int kprime,int k,double phikprime,double phik,String algorithm,BiFunction<Clustering,Clustering, Boolean> f)
    {
		int kmedio = (int) (kprime + k)/2;
		if(kmedio==kprime)return C2;
		Clustering Cmedio = ClusteringBuilder.clusteringAlgorithm(C.getPoints(), kmedio, f, algorithm);
		double phikmedio = Cmedio.objectiveFunction(algorithm);
    		
    		if(f.apply(Cmedio, C2))
    			return bisection(Cmedio, C2, kmedio, k, phikmedio, phik, algorithm, f);
    		else
    			return bisection(C, Cmedio, kprime, kmedio, phikprime, phikmedio, algorithm, f);
    }
    
}
