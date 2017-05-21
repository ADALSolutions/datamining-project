package it.unipd.dei.dm1617;

import it.unipd.dei.dm1617.Distance;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.io.Serializable;
import java.util.Random;

public class Clustering implements Serializable
{
	
    private ArrayList<Point> P; // Points that belong to the clustering
    private ArrayList<Cluster> clusters; // Clusters that belong to the clustering
    private HashMap<Point, Cluster> map;

    public Clustering(ArrayList<Point> P, ArrayList<Cluster> clusters, HashMap<Point, Cluster> map, ArrayList<Point> centers) {
        this.P = P;
        this.clusters = clusters;
        this.map = map;
        /*HashMap<Cluster,Point> mapCenters=new HashMap<Cluster,Point>();
        for(int i=0;i<centers.size();i++)
        {
            mapCenters.put(clusters.get(i), centers.get(i));
        }
        this.setCenters(mapCenters);*/
    }   
    
    public Clustering(ArrayList<Point> P, ArrayList<Cluster> clusters, HashMap<Point, Cluster> map) {
        this.P = P;
        this.clusters = clusters;
        this.map = map;
    }    
    
    public Clustering(ArrayList<Point> P, ArrayList<Cluster> clusters, ArrayList<Point> centers) {
        this.P = P;
        this.clusters = clusters;
        this.map = this.init(clusters);
        /*HashMap<Cluster,Point> mapCenters=new HashMap<Cluster,Point>();
        for(int i=0;i<centers.size();i++)
        {
            mapCenters.put(clusters.get(i), centers.get(i));
        }
        this.setCenters(mapCenters);*/        
    }
    
    public Clustering(ArrayList<Point> P, ArrayList<Cluster> clusters) {
        this.P = P;
        this.clusters = clusters;
        this.map = this.init(clusters);
    }
    
    public Clustering(ArrayList<Point> P) {
        this.P = P;
        this.clusters = new ArrayList<Cluster>();
        this.map = this.init(clusters);
    }    
    
    public ArrayList<Point> getPoints() {
        return  P;
    }
    
    public ArrayList<Cluster> getClusters() {
        return clusters;
    }
    
    public Cluster getCluster(Point p) {
        return map.get(p);
    } 
    
    public ArrayList<Point> getCenters() {
        ArrayList<Point> centroids=new ArrayList<Point>();
        for(Cluster c : clusters) {
            centroids.add(c.getCenter());
        }
        return centroids;        
    }
    
    public ArrayList<Point> getCentroids(){
    	ArrayList<Point> centroids=new ArrayList<Point>();
        for(Cluster c : clusters) {
            centroids.add(c.calculateCentroid());
        }
        return centroids; 
    }

    public int getK() {
        return this.clusters.size();
    }
    
    public int size() {
    	return this.P.size();
    }
	
    public Point getRandom() {
    	Random rand = new Random();
	    Point randomElement = this.P.get(rand.nextInt(this.P.size()));
	    return randomElement;
    }
    
    public boolean addPoint(Point p,Cluster c) {
        if(!map.containsKey(p)) {
            P.add(p);
            map.put(p, c);
            c.getPoints().add(p);
            return true;
        }
        else {
            return false;
        }
    }
    
    public void addCluster(Cluster cl) {
        clusters.add(cl);
        ArrayList<Point> points = cl.getPoints();
        for(Point p : points) {
            boolean add = this.addPoint(p, cl);
            if(add == false) {
                this.removePoint(p);
                this.addPoint(p, cl);
            }
        }
    }
    
    public void assignPoint(Point p, Cluster c) {
        map.get(p).getPoints().remove(p);
        map.put(p, c);
        c.getPoints().add(p); 
    }
    
    public void removePoint(Point p) {
    	P.remove(p);
        Cluster cl = map.get(p);
        cl.getPoints().remove(p);
        map.remove(p);
    }
    
    // setPoints e setClusters forse conviene fare un clustering o rieseguire un init
    public double kmeans() {
        double sum = 0;	
        for(int j = 0; j <= this.clusters.size() - 1; j++) {
            Cluster C = this.clusters.get(j);
            ArrayList<Point> points = C.getPoints();
            Point centroid = C.getCenter();
            for(int l = 0; l < points.size(); l++) {
                sum += Math.pow(Distance.calculateDistance(centroid.parseVector(), points.get(l).parseVector(), "standard"), 2);
            }
        }
        return sum;
    }
    
    public double kcenter() {
        double max = Distance.calculateDistance(this.clusters.get(0).getCenter().parseVector(),
                this.clusters.get(0).getPoints().get(0).parseVector(), "standard");
        for(int j = 0; j <= this.clusters.size() - 1; j++) {
            Cluster C = this.clusters.get(j);
            ArrayList<Point> points = C.getPoints();
            Point centroid = C.getCenter();
            for(int l = 0; l < points.size(); l++) {
                double dist = Distance.calculateDistance(centroid.parseVector(), points.get(l).parseVector(), "standard");
                if(dist > max){    				
                        max = dist;    				
                }                                
            }
        }
        return max;
    }
    
    public double kmedian() {
        double sum = 0;	
        for(int j = 0; j <= this.clusters.size() - 1; j++) {
            Cluster C = this.clusters.get(j);
            ArrayList<Point> points = C.getPoints();
            Point centroid = C.getCenter();
            for(int l = 0; l < points.size(); l++) {
                sum += Distance.calculateDistance(centroid.parseVector(), points.get(l).parseVector(), "standard");
            }
        }
        return sum;
    }  
    
    public double objectiveFunction(String objectiveFunction){
      if (objectiveFunction.equals("kcenter")){
  		  return this.kcenter();
  	  }
  	  else if (objectiveFunction.equals("kmeans")){
  		  return this.kmeans();
  	  }
  	  else if (objectiveFunction.equals("kmedian")){
  		  return this.kmedian();
  	  }
      return -1;
    }
    
    public HashMap<Point, Cluster> init(ArrayList<Cluster> clusters){
        HashMap<Point, Cluster> map = new HashMap<Point, Cluster>();
        for(Cluster cl : clusters) {
            for(Point p : cl.getPoints()) {
                map.putIfAbsent(p, cl);
            }
        }
        return map; 
    }
    
    public void init(ArrayList<Point> P, ArrayList<Cluster> clusters) {
        this.P = P;
        this.clusters = clusters;
        this.map = init(clusters);
    }    
    
    
    //  clusters must be passed ordered
    public void setCenters(HashMap<Cluster,Point> mapCenters)
    {
    	for(Cluster c:mapCenters.keySet()){
    		c.setCenter(mapCenters.get(c));
    		
    	}
    }
    //Returns a shallow copy of this Clustering instance.
    public Clustering clone()
    {
        ArrayList<Point> Pclone=(ArrayList<Point>) P.clone();
        HashMap<Point, Cluster> mapClone=(HashMap<Point, Cluster>) map.clone();
        ArrayList<Cluster> clustersClone=(ArrayList<Cluster>) clusters.clone();
        //HashMap<String,Object> addInfoClone=(HashMap<String,Object>) this.additionalInformation.clone();
        Clustering Cclone=new Clustering(Pclone,clustersClone,mapClone);
        //Cclone.additionalInformation=addInfoClone;
        return Cclone;
    }

}