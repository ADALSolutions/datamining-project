package GoalA;

import java.util.Vector;

public abstract class Point<T> {
 
	// Assuming Points = Vectors
    private Vector<T> p;
    private Cluster C; // Optional -> a point can be identified uniquelly by the cluster it belongs to
    org.apache.spark.mllib.linalg.Vector v;

    public Point(Vector<T> p, Cluster C) {
        this.p = p;
        this.C = C;
    }

    public Point(Vector<T> p) {
        this.p = p;
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
    
    /**
     *
     * @return
     */
    public abstract org.apache.spark.mllib.linalg.Vector parseVector(java.util.Vector v);
    
    public org.apache.spark.mllib.linalg.Vector parseVector(){return this.parseVector((java.util.Vector) this.v);}
}
