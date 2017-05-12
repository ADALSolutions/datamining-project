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
    
    public static double[] toDoubleArray(java.util.Vector v) // Una fantastica porcata
    {
        double[] dd=new double[v.size()];
        for( int i=0;i<v.size();i++)
        {
           dd[i] =(Double)v.get(i);
        }
        return dd;
    }
    public static java.util.Vector toVector(double[]  dd)
    {
        java.util.Vector v=new java.util.Vector(dd.length);
        for( int i=0;i<dd.length;i++)
        {
           v.add(dd[i]);
        }
        return v;       
    }
    
    /*
    //Initializes the process
    public void init() {
    	//Create Points
    	points = Point.createRandomPoints(MIN_COORDINATE,MAX_COORDINATE,NUM_POINTS);
    	
    	//Create Clusters
    	//Set Random Centroids
    	for (int i = 0; i &lt; NUM_CLUSTERS; i++) {
    		Cluster cluster = new Cluster(i);
    		Point centroid = Point.createRandomPoint(MIN_COORDINATE,MAX_COORDINATE);
    		cluster.setCentroid(centroid);
    		clusters.add(cluster);
    	}
    	
    	//Print Initial state
    	plotClusters();
    }
 
	private void plotClusters() {
    	for (int i = 0; i &lt; NUM_CLUSTERS; i++) {
    		Cluster c = clusters.get(i);
    		c.plotCluster();
    	}
    }
    
	//The process to calculate the K Means, with iterating method.
    public void calculate() {
        boolean finish = false;
        int iteration = 0;
        
        // Add in new data, one at a time, recalculating centroids with each new one. 
        while(!finish) {
        	//Clear cluster state
        	clearClusters();
        	
        	List lastCentroids = getCentroids();
        	
        	//Assign points to the closer cluster
        	assignCluster();
            
            //Calculate new centroids.
        	calculateCentroids();
        	
        	iteration++;
        	
        	List currentCentroids = getCentroids();
        	
        	//Calculates total distance between new and old Centroids
        	double distance = 0;
        	for(int i = 0; i &lt; lastCentroids.size(); i++) {
        		distance += Point.distance(lastCentroids.get(i),currentCentroids.get(i));
        	}
        	System.out.println("#################");
        	System.out.println("Iteration: " + iteration);
        	System.out.println("Centroid distances: " + distance);
        	plotClusters();
        	        	
        	if(distance == 0) {
        		finish = true;
        	}
        }
    }
    
    private void clearClusters() {
    	for(Cluster cluster : clusters) {
    		cluster.clear();
    	}
    }
    
    private List getCentroids() {
    	List centroids = new ArrayList(NUM_CLUSTERS);
    	for(Cluster cluster : clusters) {
    		Point aux = cluster.getCentroid();
    		Point point = new Point(aux.getX(),aux.getY());
    		centroids.add(point);
    	}
    	return centroids;
    }
    
    private void assignCluster() {
        double max = Double.MAX_VALUE;
        double min = max; 
        int cluster = 0;                 
        double distance = 0.0; 
        
        for(Point point : points) {
        	min = max;
            for(int i = 0; i &lt; NUM_CLUSTERS; i++) {
            	Cluster c = clusters.get(i);
                distance = Point.distance(point, c.getCentroid());
                if(distance &lt; min){
                    min = distance;
                    cluster = i;
                }
            }
            point.setCluster(cluster);
            clusters.get(cluster).addPoint(point);
        }
    }
    
    private void calculateCentroids() {
        for(Cluster cluster : clusters) {
            double sumX = 0;
            double sumY = 0;
            List list = cluster.getPoints();
            int n_points = list.size();
            
            for(Point point : list) {
            	sumX += point.getX();
                sumY += point.getY();
            }
            
            Point centroid = cluster.getCentroid();
            if(n_points &gt; 0) {
            	double newX = sumX / n_points;
            	double newY = sumY / n_points;
                centroid.setX(newX);
                centroid.setY(newY);
            }
        }
    }
    
    
    public static void main(String[] args) {
    	
    	KMeans kmeans = new KMeans();
    	kmeans.init();
    	kmeans.calculate();
    }
    
    */
    
}