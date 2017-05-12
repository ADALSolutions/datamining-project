package GoalA;

import java.util.ArrayList;
 
public class Cluster {
	
	public ArrayList<Point> P; // Points that belong to the Cluster
	// Assuming Center-based Cluster
	public Point c; // centroid
	public int id; // Also this could be optional
	
	public Cluster(int id) {
		this.P = new ArrayList<Point>();
		this.c = null;
		this.id = id;
	}
 
	public void addPoint(Point p) {
		P.add(p);
	}
	
	public ArrayList<Point> getPoints() {
		return P;
	}
	
	public void setCentroid(Point c) {
		this.c = c;
	}
 
	public Point getCentroid() {
		return this.c;
	}
 
	public int getId() {
		return this.id;
	}
	
	/* 
	 	
	public void plotCluster() {
	}
	
	*/
 
}