/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package GoalA;

import java.util.ArrayList;
import java.util.Random;
import org.apache.spark.mllib.linalg.Vector;

/**
 *
 * @author DavideDP
 */
public class ClusteringBuilder 
{
    public static Clustering PARTITION(ArrayList<Point> P, ArrayList<Point> S,int k )
    {
    if (P.containsAll(S) && S.size()==k ) {
      throw new IllegalArgumentException("S must have size k and be a subset of P");
    }  
    
    ArrayList<Cluster> clusters =new ArrayList<Cluster> ();   
    	// for i <- 1 to k do C_i <- null
    	for(int i = 0; i < k; i++){
    		Cluster C = new Cluster();
    		clusters.add(C);
    	}
        if(k==1)return new Clustering(P,clusters,S,k);	
    	for(Point p: P){    
                Vector parseVector = p.parseVector();
    		// argmin computation
                //trovare un modo per applicare tutte le distanze in automatico,tipo un lemma tra i parametri
    		double min = Distance.cosineDistance(parseVector,S.get(0).parseVector());
                int argmin=0;
    		for(int i = 1; i <= k - 1; i++){    
    			// Assuming Points = Vectors
    			double dist = Distance.cosineDistance(parseVector,S.get(i).parseVector());		
    			if(dist < min){    				
    				min = dist;
    				argmin = i;    				
    			}   			
    		}
                Cluster C=clusters.get(argmin);
                p.setCluster(C);
                C.addPoint(p);
                
    	}
        return new Clustering(P,clusters,S,k);        
    }
    
    public static Clustering FarthestFirstTraversal(ArrayList<Point> P,int k)
    {
            /*Clustering clustering=new Clustering( P,k);
	    Point first = clustering.getRandom();
            ArrayList<Point> S=clustering.getCentroids();
	    clustering.addCentroid(first);
	    clustering.removePoint(first);
             */

            // rimuovo i punti dal clustering in modo da tener traccia dei rimanenti P\cc
	    // poi li riaggiungo tutti alla fine, credo sia più efficiente
	    Point first = P.get(new Random().nextInt(P.size()));
            ArrayList<Point> S=new  ArrayList<Point>();
            S.add(first);
            P.remove(first);
	     //se p.size<k allora raise error 
	    for(int i = 0; i <= k - 2; i++)
            {
    		//prendo come inizio sempre la distanza tra il primo centroide e il primo punto
    		double max =Distance.cosineDistance(first.parseVector(),P.get(0).parseVector());
                int argmax=0;		
    		for(int j = 0; j <= P.size() - 1; j++)
                {
                    Point esamina=P.get(j);
                    for(int l=0;l<S.size();l++)
                    {
    			double dist = Distance.cosineDistance(esamina.parseVector(),S.get(l).parseVector());		
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
            
            return ClusteringBuilder.PARTITION(P, S, k);
    }    
    
    
    public static Clustering kmeansAlgorithm(ArrayList<Point> P,int k)
    {
        Clustering primeClustering=new Clustering( P,k);
        boolean stopping_condition=false;
        initRandomCentroids(primeClustering,k);
        double phi = Double.MAX_VALUE;
        boolean stoppingCondition = false;

        while (!stoppingCondition)
        {
            primeClustering = ClusteringBuilder.PARTITION(P, primeClustering.getCentroids(), k);
            ArrayList<Cluster> clusters = primeClustering.getClusters();
            ArrayList<Point> primeCentroids=new  ArrayList<Point>();
            //N.B quando aggiorno i centroidi dei cluster non aggiorno in automatico la lista dei centroidi di Clustering,
            //ma lo faccio in un secondo momento con primeCentroids
            for(int i = 0; i <= clusters.size() - 1; i++)
            {
                Point centroid = clusters.get(i).calculateCentroid();
                clusters.get(i).setCentroid(centroid);
                primeCentroids.add(centroid);
            }
            double phikmeans=primeClustering.kmeans();
            if( phi> phikmeans)
            {
                phi=phikmeans;
                primeClustering.setCentroids(primeCentroids);
            }
            else
            {
                stoppingCondition = true;
            }
        }
        return primeClustering;
        
    }
    
    //scrive nuovi centroidi dentro al clustering
    public static void initRandomCentroids(Clustering primeClustering,int k)
    {
        for(int i = 0; i <= k - 1; i++)
        {
                Point centroid = primeClustering.getRandom();
                primeClustering.addCentroid(centroid);
        }        
    }
    public static void kmeansPlusPlus(Clustering primeClustering,int k)
    {
        
    }
    public static Clustering PartitioningAroundMedoids(ArrayList<Point> P,ArrayList<Point> S,int k)
    {
        Clustering C_init = ClusteringBuilder.PARTITION(P, S,k);
        boolean stopping_condition = false;
        
        while (!stopping_condition)
        {
            stopping_condition = true;
            P.removeAll(S);
            boolean condition_exit=false;
            for (int i=0;i<P.size();i++)
            {
                if(condition_exit)break;
                Point p=P.get(i);
                ArrayList<Point> Sprimo=(ArrayList<Point>) S.clone();
                for(int j=0;j<S.size();j++) 
                {
                    Point c=S.get(j);
                    Sprimo.remove(c);
                    Sprimo.add(p);
                    P.addAll(S);
                    Clustering Cprimo = ClusteringBuilder.PARTITION(P, Sprimo, k);
                    double phimedianPrimo=Cprimo.kmedian();
                    double phimedian=C_init.kmedian();
                    if (phimedianPrimo<phimedian)
                    {
                        stopping_condition = false;
                        C_init=Cprimo;
                        condition_exit=true;
                        break;
                    }
                }
            }
        }
        return C_init;        
    }    

    
    
    
    
    
    
    
    
    
            
    
    
    
    
    
    
    
    
        
    public static double[] toDoubleArray(ArrayList v) // Una fantastica porcata
    {
        double[] dd=new double[v.size()];
        for( int i=0;i<v.size();i++)
        {
           dd[i] =(Double)v.get(i);
        }
        return dd;
    }
    
    public static ArrayList toVector(double[]  dd)
    {
        java.util.ArrayList v=new java.util.ArrayList(dd.length);
        for( int i=0;i<dd.length;i++)
        {
           v.add(dd[i]);
        }
        return v;       
    }
}
