/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package GoalA;

import java.util.ArrayList;
import java.util.Random;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 *
 * @author DavideDP
 */
public class Evaluation
{
    public static Vector generateRandomVector(int size)
    {
        double dd[]=new double[size];
        Random r=new Random();
        for(int i=0;i<size;i++)
        {
            dd[i]=r.nextDouble();
        }
        return Vectors.dense(dd);
    }
    public static double HopkinsStatistic(ArrayList<Point> P,ArrayList<Point> sample)
    {
        //sample.size>0
        ArrayList<Point> generated=new ArrayList<Point>(sample.size());
        int sizeVector=sample.get(0).parseVector().size();
        for(int i=0;i<sample.size();i++)
        {
            //genero numeri troppo grandi secondo me
            generated.add(new PointCentroid(Evaluation.generateRandomVector(sizeVector)));
        }
        P.removeAll(sample);//credo vadano tolti
        ArrayList<Double> w=new ArrayList<Double>();
        ArrayList<Double> u=new ArrayList<Double>();
        for(int i=0;i<sample.size();i++)
        {
            Point x=sample.get(i);
            Point y=generated.get(i);
            double minw=Distance.cosineDistance(P.get(0).parseVector(),x.parseVector());
            double minu=Distance.cosineDistance(P.get(0).parseVector(),y.parseVector());
            for(int j=1;j<P.size();j++)
            {
                double distw=Distance.cosineDistance(P.get(j).parseVector(),x.parseVector());
                double distu=Distance.cosineDistance(P.get(j).parseVector(),y.parseVector());
                if(distw<minw)minw=distw;
                if(distu<minu)minu=distu;
            }
            w.add(minw);
            u.add(minu);
        }  
        double sumu=0;
        double sumw=0;
        for(int i=0;i<sample.size();i++)
        {
            sumu+=u.get(i);
            sumw+=w.get(i);
        }
        double H= sumw/(sumu+sumw);
        return H;
        /*If H is close to 0, then P is likely to have a clustering structure, while if
H is close to 1, then the points of P are likely to be well (i.e., regularly)
spaced. If H is close to 0:5, then P is likely to be a random set.*/
    }
    
}
