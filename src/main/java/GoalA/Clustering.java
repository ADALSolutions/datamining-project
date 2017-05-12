package GoalA;

import GoalA.Distance;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.ArrayList;
import java.util.Random;

public class Clustering {
	
    public int k;
    private ArrayList<Point> P; // Points that belong to the clustering
    private ArrayList<Cluster> clusters; // Clusters that belong to the clustering
    private ArrayList<Point> centroids; // Centroids that belong to the clustering {public void partition()}
    
    public Clustering(int k) {
    	this.k = k;
    	this.P = new ArrayList<Point>();
    	this.clusters = new ArrayList<Cluster>();  
    	this.centroids = new ArrayList<Point>();
    }

    public Clustering( ArrayList<Point> P, ArrayList<Cluster> clusters, ArrayList<Point> centroids,int k) {
        this.k = k;
        this.P = P;
        this.clusters = clusters;
        this.centroids = centroids;
    }
    public Clustering( ArrayList<Point> P,int k) {
        this.k = k;
        this.P = P;
        this.clusters = new ArrayList<Cluster>();
        this.centroids = new ArrayList<Point>();
    }    
    public void addPoint(Point p) {
		P.add(p);
	}
    
	public ArrayList<Point> getPoints() {
		return P;
	}
	
	public void addCluster(Cluster C){
		clusters.add(C);
	}
	
	public ArrayList<Cluster> getClusters(){
		return clusters;
	}
	
	public void addCentroid(Point c){
		centroids.add(c);
	}
    
	public ArrayList<Point> getCentroids(){
		return centroids;
	}
	
	public int getK() {
		return this.k;
	}
	
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
    
    
    public Point getRandom(){
    	Random rand = new Random();
	    Point randomElement = this.P.get(rand.nextInt(this.P.size()));
	    return randomElement;
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
        public void removePoint(Point p) {
    	P.remove(p);
    }
    
    public Point getPoint(int i){
    	return P.get(i);
    }
    
    public void setPoints(ArrayList<Point> P){
    	this.P = P;
    }
    


    public ArrayList<Point> getP() {
        return P;
    }

    public void setP(ArrayList<Point> P) {
        this.P = P;
    }
    
    public static Clustering FFT(ArrayList<Point> P,int k)
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
            
            return Clustering.PARTITION(P, S, k);
    }
    
	
	public Cluster getCluster(int i){
		return this.clusters.get(i);
	}
        
        	public void setK(int k){
		this.k = k;
	}
                
        public void setCentroids(ArrayList<Point> cc){
		this.centroids = cc;
	}
        
        public Point getCentroid(int i){
		return this.centroids.get(i);
	}
        
        
        public void setClusters(ArrayList<Cluster> CC){
		this.clusters = CC;
	}
    

	

}