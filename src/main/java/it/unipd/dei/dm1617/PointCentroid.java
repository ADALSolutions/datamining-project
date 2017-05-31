/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import java.util.ArrayList;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 *
 * @author DavideDP
 */
public class PointCentroid extends Point
{
    Vector parse;
    
    public PointCentroid(ArrayList v) {
        super();
        this.parse = parseVector(v);
    }
    
    public PointCentroid(Vector v) {
        super();
        this.parse = v;
    }    
    
    @Override
    public Vector parseVector(ArrayList a) {
        return Vectors.dense(Utility.toDoubleArray(a));
    }
    
    public Vector parseVector() {
        return parse;
    }    
    public void assignVector(Vector v) {
        parse=v;
    }        
    
    public Point copy()
    {
        PointCentroid p = new PointCentroid(parse.copy());
        p.setDist(getDist());
        return p;
    }
    
    
}
