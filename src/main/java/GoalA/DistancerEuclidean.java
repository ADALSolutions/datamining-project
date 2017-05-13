/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package GoalA;

import org.apache.spark.mllib.linalg.Vector;

/**
 *
 * @author DavideDP
 */
public class DistancerEuclidean extends Distancer 
{
    double r;

    public DistancerEuclidean(double r) {
        this.r = r;
    }
    
    @Override
    public double calculateDistance(Vector a, Vector b) 
    {
       if(r==1)return Distance.manhattanDistance(a, b);
       if(r==2)return Distance.standardDistance(a, b);
       return Distance.euclideanDistance(a, b, (int) r);
    }
    
}
