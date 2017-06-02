package it.unipd.dei.dm1617;

import java.util.ArrayList;
import java.io.Serializable;
import java.util.Objects;

public abstract class Point implements Serializable,Comparable{
 
    protected String ID;
    public static int ID_static=0;
    private double dist;//Attributo Aggiunto per metodo euristico
    private String ID_Cluster;
    public Point() {
        ID=String.valueOf(ID_static);
        ID_static++;
    }
    
    public abstract org.apache.spark.mllib.linalg.Vector parseVector(ArrayList v);
    public abstract org.apache.spark.mllib.linalg.Vector parseVector();
        
    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 53 * hash + Objects.hashCode(this.ID);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Point other = (Point) obj;
        if (!Objects.equals(this.ID, other.ID)) {
            return false;
        }
        return true;
    }

    public abstract Point copy();
    
    public double getDist() {
        return dist;
    }

    public void setDist(double dist) {
        this.dist = dist;
    }

    @Override
    public int compareTo(Object obj) 
    {
        if (this == obj) {
            return 0;
        }
        if (obj == null) {
            return 0;
        }
        if (getClass() != obj.getClass()) {
            return 0;
        }
        final Point other = (Point) obj;
        if(this.dist>other.getDist()) return 1;
        if(this.dist==other.getDist()) return 0;
        return -1;
    }

    public String getID_Cluster() {
        return ID_Cluster;
    }

    public void setID_Cluster(String ID_Cluster) {
        this.ID_Cluster = ID_Cluster;
    }
    
    
    
    
    
    
}
