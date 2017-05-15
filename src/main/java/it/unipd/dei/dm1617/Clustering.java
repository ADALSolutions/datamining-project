package it.unipd.dei.dm1617;

import it.unipd.dei.dm1617.Distance;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

public class Clustering 
{
	
    private ArrayList<Point> P; // Points that belong to the clustering
    private ArrayList<Cluster> clusters; // Clusters that belong to the clustering
    private ArrayList<Point> centroids;
    private HashMap<Point, Cluster> map;
    public boolean editCentroids = true;

    public Clustering(ArrayList<Point> P, ArrayList<Cluster> clusters, HashMap<Point, Cluster> map, ArrayList<Point> centroids) {
        this.P = P;
        this.clusters = clusters;
        this.map = map;
        this.centroids = centroids;
    }   
    
    public Clustering(ArrayList<Point> P, ArrayList<Cluster> clusters, HashMap<Point, Cluster> map) {
        this.P = P;
        this.clusters = clusters;
        this.map = map;
        this.centroids = this.getCentroids();
    }    
    
    public Clustering(ArrayList<Point> P, ArrayList<Cluster> clusters, ArrayList<Point> centroids) {
        this.P = P;
        this.clusters = clusters;
        this.map = this.init(clusters);
        this.centroids = this.getCentroids();
        this.centroids = centroids;
    }
    
    public Clustering(ArrayList<Point> P, ArrayList<Cluster> clusters) {
        this.P = P;
        this.clusters = clusters;
        this.map = this.init(clusters);
        this.centroids = this.getCentroids();
        this.centroids = this.getCentroids();
    }
    
    public Clustering(ArrayList<Point> P) {
        this.P = P;
        this.clusters = new ArrayList<Cluster>();
        this.map = this.init(clusters);
        this.centroids = this.getCentroids();
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
    
    public ArrayList<Point> getCentroids() {
        if(this.editCentroids) {
            ArrayList<Point> centroids=new ArrayList<Point>();
            for(Cluster c : clusters) {
                centroids.add(c.getCentroid());
            }
            return centroids;
        }
        else {
            return centroids;
        }
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
            c.setEdit(true);
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
        cl.setEdit(true);
        map.remove(p);
    }
    
    // setPoints e setClusters forse conviene fare un clustering o rieseguire un init
    public double kmeans() {
        double sum = 0;	
        for(int j = 0; j <= this.clusters.size() - 1; j++) {
            Cluster C = this.clusters.get(j);
            ArrayList<Point> points = C.getPoints();
            Point centroid = C.getCentroid();
            for(int l = 0; l < points.size(); l++) {
                sum += Math.pow(Distance.calculateDistance(centroid.parseVector(), points.get(l).parseVector(), "standard"), 2);
            }
        }
        return sum;
    }
    
    public double kcenter() {
        double max = Distance.calculateDistance(this.clusters.get(0).getCentroid().parseVector(),
                this.clusters.get(0).getPoints().get(0).parseVector(), "standard");
        for(int j = 0; j <= this.clusters.size() - 1; j++) {
            Cluster C = this.clusters.get(j);
            ArrayList<Point> points = C.getPoints();
            Point centroid = C.getCentroid();
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
            Point centroid = C.getCentroid();
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
    
    public void setCentroids(ArrayList<Point> centroids)
    {
        this.editCentroids = false;
        this.centroids = centroids;  
    }

}