package GoalA;

import java.util.ArrayList;

public abstract class Point<T> {
 
	// Assuming Points = Vectors
    private ArrayList<T> point;
    private Cluster C; // Optional -> a point can be identified uniquelly by the cluster it belongs to

    public Point(ArrayList<T> p, Cluster C) {
        this.point = p;
        this.C = C;
    }
    public Point() {
        this.point = new ArrayList<T>();
        this.C = null;
    }
    public Point(ArrayList<T> p) {
        this.point = p;
    }
    
    public void setCluster(Cluster C) {
        this.C = C; // Assigning a Cluster to belong by reference
    }
    
    public Cluster getCluster() {
        return this.C;
    }
    
    public void setArrayList(ArrayList v){
    	this.point = (ArrayList) v.clone();
    }
    
    public ArrayList getArrayList(){
    	return this.point;
    }
    
    public abstract org.apache.spark.mllib.linalg.Vector parseVector(ArrayList v);
    
    public org.apache.spark.mllib.linalg.Vector parseVector(){return this.parseVector((ArrayList) this.point);}
}
