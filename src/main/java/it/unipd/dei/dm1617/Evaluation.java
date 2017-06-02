/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

/**
 *
 * @author DavideDP
 */
public class Evaluation {
	
    public static Vector generateRandomVector(int size) {
        double dd[] = new double[size];
        Random r = new Random();
        for(int i = 0; i < size; i++) {
            dd[i] = r.nextDouble()*Double.MAX_VALUE;
        }
        return Vectors.dense(dd);
    }
        /*If H is close to 0, then P is likely to have a clustering structure, while if
        H is close to 1, then the points of P are likely to be well (i.e., regularly)
        spaced. If H is close to 0:5, then P is likely to be a random set.*/    
    public static double HopkinsStatistic(ArrayList<Point> P, ArrayList<Point> X) {
        //sample.size>0
        int t=X.size();
        //CREO Y
        ArrayList<Point> generated = new ArrayList<Point>(t);
        int sizeVector = X.get(0).parseVector().size();
        for(int i = 0; i < t; i++) {
            //genero numeri troppo grandi secondo me
            generated.add(new PointSpark(Evaluation.generateRandomVector(sizeVector)));
        }
        //CALCOLO w_i e u_i
        //P.removeAll(X);//credo vadano tolti altrimenti per se stesso il punto ha dist=0
        ArrayList<Double> w = new ArrayList<Double>();
        ArrayList<Double> u = new ArrayList<Double>();
        for(int i = 0; i < t; i++) {
            Point x = X.get(i);
            Point y = generated.get(i);
            double minw = Distance.calculateDistance(P.get(0).parseVector(), x.parseVector(), "standard");
            double minu = Distance.calculateDistance(P.get(0).parseVector(), y.parseVector(), "standard");
            for(int j = 1; j < P.size(); j++) {
                double distw = Distance.calculateDistance(P.get(j).parseVector(), x.parseVector(), "standard");
                double distu = Distance.calculateDistance(P.get(j).parseVector(), y.parseVector(), "standard");
                if(distw < minw && P.get(0).equals(x)==false) {
                	minw = distw;
                }
                if(distu < minu && P.get(0).equals(y)==false) {
                	minu = distu;
                }
            }
            w.add(minw);
            u.add(minu);
        } 
        /*
        Fa la stessa cosa di sopra ma scrittura più compatta
        for(int i = 0; i < t; i++) {            
            Point x = X.get(i);
            Point y = generated.get(i);
            Point minx=ClusteringBuilder.mostClose(P,x)._2;
            Point miny=ClusteringBuilder.mostClose(P,y)._2;
            w.add(Distance.calculateDistance(minx.parseVector(), x.parseVector(), "standard"));
            u.add(Distance.calculateDistance(miny.parseVector(), x.parseVector(), "standard"));
        }
        */
        double sumu = 0;
        double sumw = 0;
        for(int i = 0; i < X.size(); i++){
            sumu += u.get(i);
            sumw += w.get(i);
        }
        double H = sumw / (sumu + sumw);
        return H;

    }
    
    //dato che si ripete per tutti i punti credo si possa ottimizzare in qualche modo
    //The value of sp varies between -1 and 1.
    //The closer sp to 1 the better.
    public static double SilhouetteCoefficient(Clustering C, Point p) {
       double ap = C.getCluster(p).averageDistance(p);
       double min = Double.MAX_VALUE;
       for(int i = 0; i < C.getClusters().size(); i++) {
           if(!C.getCluster(p).equals(C.getClusters().get(i))) {
               double bp = C.getCluster(p).averageDistance(p);
               if(min > bp) {
            	   min = bp;
               }
           }
       }
       double bp = min;
       double sp = (bp - ap) / Math.max(ap, bp);
       return sp;
    }
    
    //average silhouette coecient over all points of P.
    public static double AverageSilhouetteCoefficient(Clustering C) {
       double sum = 0;
       for(Point p : C.getPoints()) {
           sum += Evaluation.SilhouetteCoefficient(C, p);
       }
       return sum / C.getPoints().size();
    }
    
    
    //complesso
    /*
    It measures the impurity of C, ranging from 0 (i.e., min impurity when all
    points of C belong to the same class), to log2 L (i.e., max impurity when
    all classes are equally represented in C).
    */
    //Invece di calcolare l'entropia per un certo cluster, la calcolo per tutti i clusters
    public static double AverageEntropyCluster(Clustering c, HashMap<Point, String> label) 
    {
        //Calcolo mC=#points in cluster C,io lo faccio per ogni cluster
        HashMap<Cluster, Integer> mC = new HashMap<Cluster, Integer>();
        for(Cluster cluster : c.getClusters()) {
        	mC.put(cluster, cluster.getPoints().size());
        }
        //In questa parti calcolo m_i=#points of class i e m_C_i=#points of class i in C
        //metto UpdatableNumber invece di Double per velocizzare
        HashMap<String, UpdatableNumber> m_i = new HashMap<String, UpdatableNumber>();
        HashMap<Cluster, HashMap<String, UpdatableNumber>> m_C_i = new HashMap<Cluster, HashMap<String, UpdatableNumber>>();
        Iterator it = label.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();//<Point,String>
            UpdatableNumber get = m_i.getOrDefault(pair.getValue(), new UpdatableNumber());
            get.add(1);
            HashMap<String, UpdatableNumber> get2 = m_C_i.getOrDefault(c.getCluster((Point)pair.getKey()), new HashMap<String, UpdatableNumber>());                                                     
            UpdatableNumber get3 = get2.getOrDefault(pair.getValue(), new UpdatableNumber());
            get3.add(1);
        }
        Set<String> classLabel = m_i.keySet();
        //Calcolo Entropia
        //N.B volendo posso restituire l'entropia media per ogni cluster come ArrayList
        double average = 0;
        //volendo posso fare un hasmap e salvare l'entropia di ogni cluster
        for(Cluster cluster : c.getClusters()) {
            double entropyCluster = 0;
            for(String s : classLabel) {
                double p = m_C_i.get(cluster).get(s).getNumber() / mC.get(cluster);
                entropyCluster += p * (Math.log(p) / Math.log(2));
            }
            average += entropyCluster;
        }
        average = average / c.getClusters().size();
        return average;
    }
    
    /*
    It measures whether the clustering is in accordance with the
    partition induced by the classes, ranging from 0 (no accordance) to
    1 (maximum accordance)
    */
    public static double randStatistic(Clustering C, HashMap<Point, String> label, Set<String> classLabel){
        double[][] f = Evaluation.getF(C, label, classLabel);
        //(f00+f11)/(f00+f01+f10+f11);
        return (f[0][0] + f[1][1]) / (f[0][0] + f[0][1] + f[1][0] + f[1][1]);
    }
    //restituisce nell'ordine f00,f01,f10,f11
    public static double[][] getF(Clustering C, HashMap<Point, String> label, Set<String> classLabel) {
        double f00 = 0.0, f01 = 0.0, f10 = 0.0, f11 = 0.0;
        for(Cluster cluster1 : C.getClusters()) {
            for(Cluster cluster2 : C.getClusters()) {
                for(String s : classLabel) {
                    for(String s2 : classLabel) {
                        if(s.equals(s2)) { //same class
                            if(cluster1 == cluster2) { //same cluster
                                f11 += f11(cluster1.getPoints(), label);
                            }
                            else {
                                f10 += f10(cluster1.getPoints(), cluster2.getPoints(), label);
                            }     
                        }
                        else { //distinct class
                            if(cluster1 == cluster2) { //same cluster
                                f01 += f01(cluster1.getPoints(), label);
                            }
                            else { //distinct cluster
                                f00 += f00(cluster1.getPoints(), cluster2.getPoints(), label);
                            }                            
                        }
                    }
                }
            }
        }
        double[][] d = {{f00, f01}, {f10, f11}};
        return d;
    }
    /*proportion of pairs of the same class in the same
    cluster relatively to the total number of pairs that are of the same
    class or in the same cluster.*/
    public static double JaccardStatistic(Clustering C, HashMap<Point, String> label, Set<String> classLabel) {
        double[][] f = Evaluation.getF(C, label, classLabel);
        //(f11)/(f01+f10+f11);
        return (f[1][1]) / (f[0][1] + f[1][0] + f[1][1]);
    }
    //#pairs of points of distinct classes in distinct clusters
    public static int f00(ArrayList<Point> points,ArrayList<Point> points2, HashMap<Point,String> label) {
        int f00 = 0;
        for(Point p : points) {
            for(Point q : points2) {
                if(p != q && !label.get(p).equals(label.get(q))) {
                    f00++;
                }
            }
        }
        return f00;
    }
    //#pairs of points of distinct classes in the same cluster
    public static int f01(ArrayList<Point> points, HashMap<Point, String> label) {
    int f01 = 0;
        for(Point p : points) {
            for(Point q : points) {
                if(p != q && !label.get(p).equals(label.get(q))) {
                    f01++;
                }
            }
        }
        return f01;
    } 
    //#pairs of points of the same class in distinct clusters
    public static int f10(ArrayList<Point> points, ArrayList<Point> points2, HashMap<Point, String> label) {
        int f10 = 0;
        for(Point p : points) {
            for(Point q : points2) {
                if(p != q && label.get(p).equals(label.get(q))) {
                    f10++;
                }
            }
        }
        return f10;
    } 
    
    //#pairs of points of the same class in the same cluster
    public static int f11(ArrayList<Point> points, HashMap<Point, String> label) {
        int f11 = 0;
        for(Point p : points) {
            for(Point q : points) {
                if(p != q && label.get(p).equals(label.get(q))) {
                    f11++;
                }
            }
        }
        return f11;
    }     
    
}
