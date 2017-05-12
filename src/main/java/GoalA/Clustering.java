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
	

    
    
    public Point getRandom(){
    	Random rand = new Random();
	    Point randomElement = this.P.get(rand.nextInt(this.P.size()));
	    return randomElement;
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
    
        public double kmeans()
        {
            
    		double sum=0;	
    		for(int j = 0; j <= this.clusters.size() - 1; j++)
                {
                    
                    Cluster C = this.clusters.get(j);
                    ArrayList<Point> points = C.getPoints();
                    Point centroid=C.getCentroid();
                    for(int l=0;l<points.size();l++)
                    {
    			sum+=Math.pow(Distance.cosineDistance(centroid.parseVector(),points.get(l).parseVector()),2);
                    }
    		}
                return sum;
        }
        public double kcenter()
        {
            
    		double max =Distance.cosineDistance(this.clusters.get(0).getCentroid().parseVector(),
                        this.clusters.get(0).getPoints().get(0).parseVector());
    		for(int j = 0; j <= this.clusters.size() - 1; j++)
                {
                    
                    Cluster C = this.clusters.get(j);
                    ArrayList<Point> points = C.getPoints();
                    Point centroid=C.getCentroid();
                    for(int l=0;l<points.size();l++)
                    {
    			double dist=Distance.cosineDistance(centroid.parseVector(),points.get(l).parseVector());
 			if(dist > max){    				
    				max = dist;    				
    			}                                
                    }
    		}
                return max;
        }
        public double kmedian()
        {
            
    		double sum=0;	
    		for(int j = 0; j <= this.clusters.size() - 1; j++)
                {
                    
                    Cluster C = this.clusters.get(j);
                    ArrayList<Point> points = C.getPoints();
                    Point centroid=C.getCentroid();
                    for(int l=0;l<points.size();l++)
                    {
    			sum+=Distance.cosineDistance(centroid.parseVector(),points.get(l).parseVector());
                    }
    		}
                return sum;
        }        
        

}