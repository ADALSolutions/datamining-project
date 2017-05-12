/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package GoalA;

import java.util.ArrayList;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 *
 * @author DavideDP
 */
public class PointCentroid<Double> extends Point<Double>
{
    org.apache.spark.mllib.linalg.Vector parse;
    public PointCentroid(ArrayList v) {
        super(v);
        this.parse=parseVector(v);
    }
    
    @Override
    public org.apache.spark.mllib.linalg.Vector parseVector( ArrayList a ) 
    {
        return Vectors.dense( ClusteringBuilder.toDoubleArray( a));
    }
    public org.apache.spark.mllib.linalg.Vector parseVector()
    {
        return parse;
    }    


    
}