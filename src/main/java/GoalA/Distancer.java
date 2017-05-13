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
public abstract class Distancer 
{
    public static Distancer distancer;
    public abstract double calculateDistance(Vector a,Vector b);
    public double calculateDistance(Point p,Point q)
    {
        return calculateDistance(p.parseVector(),q.parseVector());
    }
}
