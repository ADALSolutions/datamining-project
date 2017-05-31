package it.unipd.dei.dm1617;

import java.util.ArrayList;
import java.io.Serializable;
import java.util.Objects;

public abstract class Point<T> implements Serializable{
 
    protected String ID;
    public static int ID_static=0;
    
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
        final Point<?> other = (Point<?>) obj;
        if (!Objects.equals(this.ID, other.ID)) {
            return false;
        }
        return true;
    }
    
    
}
