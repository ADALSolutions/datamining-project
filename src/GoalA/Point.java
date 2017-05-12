package GoalA;

import java.util.Vector;

public class Point {
 
	// Assuming Points = Vectors
	private Vector p = new Vector(0, 0); // Constructor detail - public Vector(int initialCapacity, int capacityIncrement);
    private Cluster C; // Optional -> a point can be identified uniquelly by the cluster it belongs to
    
    public Point(Vector v)
    {
    	this.p = (Vector) v.clone();    
    	this.C = null;
    }
    
    public void setCluster(Cluster C) {
        this.C = C; // Assigning a Cluster to belong by reference
    }
    
    public Cluster getCluster() {
        return this.C;
    }
    
    public void setVector(Vector v){
    	this.p = (Vector) v.clone();
    }
    
    public Vector getVector(){
    	return this.p;
    }
    
    /*
    
    public void plotPoint(){
    }
    
    */
}
