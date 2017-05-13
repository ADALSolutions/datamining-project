package GoalA;

import java.util.ArrayList;
import java.util.HashSet;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
 
public class Cluster {
	
	public ArrayList<Point> P; // Points that belong to the Cluster
	// Assuming Center-based Cluster
	public Point centroid; // centroid
	public int id; // Also this could be optional
	
	public Cluster(int id) {
		this.P = new ArrayList<Point>();
		this.centroid = this.calculateCentroid();
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
		this.centroid = c;
	}
 
	public Point getCentroid() {
		return this.centroid;
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
        
        public double averageDistance(Point p)
        {
            double sum=0;
            boolean present=false;
            double length;
            for(int i=0;i<P.size();i++)
            {
                if(!p.equals(P.get(i)))
                {
                    
                    sum+=Distance.cosineDistance(p.parseVector(), P.get(i).parseVector());
                }
                else{present=true;}
            } 
            if(present)length=P.size()-1;
            else length=P.size();
            return sum/length;
        }
        public double cost()
        {
            Point centroid=this.centroid;
            double sum=0;
            for(Point p:this.getPoints())
            {
                sum += Math.pow(Distance.cosineDistance(p.parseVector(),centroid.parseVector()),2);
            }
            return sum;
        }
        public static Cluster union(Cluster C1,Cluster C2)
        {
            HashSet<Point> set=new HashSet(C1.getPoints());
            set.removeAll(C2.getPoints());
            ArrayList<Point> al=(ArrayList<Point>) C1.getPoints().clone();
            al.addAll(set);
            Cluster C12=new Cluster(al);//dentro di se calcolo il centroide   
            return C12;
        }

	

 
}