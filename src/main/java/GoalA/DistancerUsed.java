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
public class DistancerUsed extends Distancer
{

    @Override
    public double calculateDistance(Vector a, Vector b) 
    {
        return Distance.cosineDistance(a, b); 
    }
    
}
