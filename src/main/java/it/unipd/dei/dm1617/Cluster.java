package it.unipd.dei.dm1617;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
 
public class Cluster implements Serializable
{
	// Assuming Center-based Cluster	
    private ArrayList<Point> P; // Points that belong to the Cluster
    private Point center; // centroid
    private String ID; // this can be useful
    private static int id_static=0;

    public Cluster(ArrayList<Point> P) {
            this.P =P;
            this.center = this.calculateCentroid();
            ID="Cluster"+String.valueOf(id_static);
            id_static++;
    }
    public Cluster() {
            this.P=new ArrayList<Point>();
            this.center =null;
            ID="Cluster"+String.valueOf(id_static);
            id_static++;     
    } 
	
    public ArrayList<Point>  getPoints() 
    {
            return P;
    }     
    public int size()
    {
        return P.size();
    } 
    public Point getCenter() 
    {
            return this.center;
    }
    //serve ?
    public void setCenter(Point c) {
        this.center = c;
    }
    public Point calculateCentroid()
    {
        //se p.size==0 raise exception
        if(P.size()==0)return new PointCentroid(Vectors.zeros(2));
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

    //average distance for the point respect to the points of the cluster
    public double averageDistance(Point p)
    {
        double sum=0;
        boolean present=false;
        double length;
        for(int i=0;i<P.size();i++)
        {
            if(!p.equals(P.get(i)))
            {
                //dovrebbe essere euclidean distance con r=1
                sum+=Distance.calculateDistance(p.parseVector(), P.get(i).parseVector(), "standard");
            }
            else{present=true;}
        } 
        if(present)length=P.size()-1;
        else length=P.size();
        return sum/length;
    }
    public double cost()
    {
        Point centroid=this.center;
        double sum=0;
        for(Point p:this.getPoints())
        {
            //dovrebbe essere standardDistance
            sum += Math.pow(Distance.calculateDistance(p.parseVector(),centroid.parseVector(), "standard"),2);
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
    public String toString()
    {
        return ID;       
    }
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ID == null) ? 0 : ID.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Cluster other = (Cluster) obj;
		if (ID == null) {
			if (other.ID != null)
				return false;
		} else if (!ID.equals(other.ID))
			return false;
		return true;
	}
    
 
}