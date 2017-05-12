package GoalA;

import GoalA.Distance;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.ArrayList;
import java.util.Random;

public class Clustering {
	
    public int k;
    private ArrayList<Point> P; // Points that belong to the clustering
    private ArrayList<Cluster> CC; // Clusters that belong to the clustering
    private ArrayList<Point> cc; // Centroids that belong to the clustering {public void partition()}
    
    public Clustering(int k) {
    	this.k = k;
    	this.P = new ArrayList<Point>();
    	this.CC = new ArrayList<Cluster>();  
    	this.cc = new ArrayList<Point>();
    }
    
    public void addPoint(Point p) {
		P.add(p);
	}
    
    
	
	public ArrayList<Point> getPoints() {
		return P;
	}
	
	public void addCluster(Cluster C){
		CC.add(C);
	}
	
	public ArrayList<Cluster> getClusters(){
		return CC;
	}
	
	public void addCentroid(Point c){
		cc.add(c);
	}
    
	public ArrayList<Point> getCentroids(){
		return cc;
	}
	
	public int getK() {
		return this.k;
	}
	
    public void partition(){
    	
    	// for i <- 1 to k do C_i <- null
    	for(int i = 1; i == k; i++){
    		Cluster C = new Cluster(i);
    		this.addCluster(C);
    	}
    	
    	for(Point p: P){    		
    		// argmin computation
    		double min = Double.MAX_VALUE;
    		int argmin = Integer.MIN_VALUE; // l = argmin  			
    		for(int i = 0; i <= k - 1; i++){    
    			// Assuming Points = Vectors
    			double dist = Distance.cosineDistance(Vectors.dense(this.toDoubleArray(p.getVector())), 
    					Vectors.dense(this.toDoubleArray(cc.get(i).getVector())));		
    			if(dist < min){    				
    				min = dist;
    				argmin = i;    				
    			}   			
    		}
    		// Cluster updating
    		CC.get(argmin).addPoint(p);
    	}   	
    }
    
    
    public Point getRandom(){
    	Random rand = new Random();
	    Point randomElement = this.P.get(rand.nextInt(this.P.size()));
	    return randomElement;
    }
    
    public double[] toDoubleArray(java.util.Vector v) // Una fantastica porcata
    {
        double[] dd=new double[v.size()];
        for( int i=0;i<v.size();i++)
        {
           dd[i] =(Double)v.get(i);
        }
        return dd;
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