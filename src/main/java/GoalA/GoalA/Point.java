package GoalA;

import java.util.ArrayList;

public abstract class Point<T> {
 
	// Assuming Points = Vectors
    private ArrayList<T> point;

    public Point(ArrayList<T> p) {
        this.point = p;
    }
    
    public Point() {
        this.point = new ArrayList<T>();
    }
    
    public void setArrayList(ArrayList v) {
    	this.point = (ArrayList) v.clone();
    }
    
    public ArrayList getArrayList() {
    	return this.point;
    }
    
    public abstract org.apache.spark.mllib.linalg.Vector parseVector(ArrayList v);
    
    public org.apache.spark.mllib.linalg.Vector parseVector() {
    	return this.parseVector((ArrayList) this.point);
    }
}
