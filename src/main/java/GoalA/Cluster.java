package GoalA;

import java.util.ArrayList;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
 
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
	public Cluster() {
		this.P = new ArrayList<Point>();
	} 
	public Cluster(ArrayList<Point> ap) {
		this.P = ap;
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
        
        public Point calculateCentroid()
        {
            //se p.size==0 raise exception
            Vector y = Vectors.zeros(P.get(0).parseVector().size());
            for(int i=0;i<this.P.size();i++)
            {
                BLAS.axpy(1, P.get(i).parseVector(), y);
            }
            BLAS.scal(((double)1)/P.size(), y);
            ArrayList al=new ArrayList(y.size());
            for(int i=0;i<y.size();i++)
            {
                al.add(y.apply(i));
            }           
            return new PointCentroid(al);
        }

	

 
}