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
import org.apache.spark.mllib.linalg.Matrix;
import scala.Tuple2;

/**
 *
 * @author DavideDP
 */
public class MergingCriterion {
    //non mi serve una matrice con n^2 bensì una hasmpa con n^2/2 elementi
    //dato che matrice è simmetrica non mi servono tutti gli elementi sotto la diagonale principale
    Matrix M2;
    HashMap<Tuple2<Cluster, Cluster>, Double> M;
    Clustering C;
    
    public MergingCriterion(Clustering C, HashMap<Tuple2<Cluster, Cluster>, Double> M) {
        this.M = M;
        this.C = C;
    } 
    
    public MergingCriterion(Clustering C) {
        this.C = C;
        this.M = new HashMap<Tuple2<Cluster, Cluster>, Double> ();
        ArrayList<Cluster> clone = (ArrayList<Cluster>) C.getClusters().clone();
        for(Cluster cluster1 : C.getClusters()) {
           for(Cluster cluster2:clone) {
               double link = MergingCriterion.singleLinkage(cluster1, cluster2);
               M.put(new Tuple2<Cluster, Cluster>(cluster1, cluster2), link);
           }
           clone.remove(cluster1);
        }
    }    

    public void mergeSingleLinkage() {
        //trovo il minimo
        Tuple2<Cluster, Cluster> argmin = this.findMin(M);
        Cluster C1 = argmin._1();
        Cluster C2 = argmin._2();
        Cluster union = Cluster.union(C1, C2);
        C.getClusters().remove(C1);
        C.getClusters().remove(C2);
        C.getClusters().add(union);
        double min = M.get(argmin);
        Iterator it = M.entrySet().iterator();
        //aggiorno la tabella
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();//<Point,String>
            Tuple2<Cluster, Cluster> t = (Tuple2<Cluster, Cluster>) pair.getKey();
            Cluster cluster1 = t._1, cluster2 = t._2;
            if(t._1 == C1 || t._2 == C2){
                if(t._1 == C1 ) {
                    cluster1 = union;
                }          
                if(t._2 == C2 ) {
                    cluster2 = union;
                }
                M.remove(t);
                M.put(new Tuple2<Cluster, Cluster>(cluster1, cluster2), min);
            }
        }        
        this.C.addCluster(union);        
    }

    public Tuple2<Cluster,Cluster> findMin( HashMap<Tuple2<Cluster, Cluster>, Double> matrix)
    {
        Iterator it = matrix.entrySet().iterator();
        double min = Double.MAX_VALUE;
        Tuple2<Cluster, Cluster> argmin = null;
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();//<Point,String>
            if(min > (Double) pair.getValue()) {
                argmin = (Tuple2<Cluster, Cluster>) pair.getKey();
                min = (double) pair.getValue();
            }
        }
        return argmin;
    }
    
    public HashMap<Tuple2<Cluster, Cluster>, Double> getM() {
        return M;
    }

    public void setM(HashMap<Tuple2<Cluster, Cluster>, Double> M) {
        this.M = M;
    }

    public static double singleLinkage(Cluster C1, Cluster C2) {
        double min = Double.MIN_VALUE;
        for(Point p : C1.getPoints()) {
            for(Point q : C2.getPoints()) {
                double dist = Distance.calculateDistance(p.parseVector(), q.parseVector(), "standard");
                if(dist < min) {
                	min = dist;
                }
            }
        }
        return min;
    }
    
    public static double completeLinkage(Cluster C1, Cluster C2){
        double max = -1;
        for(Point p : C1.getPoints()) {
            for(Point q : C2.getPoints()) {
                double dist = Distance.calculateDistance(p.parseVector(), q.parseVector(), "standard");
                if(dist > max) {
                	max = dist;
                }
            }
        }
        return max;
    } 
    
    public static double averageLinkage(Cluster C1, Cluster C2) {
        double sum = 0;
        for(Point p : C1.getPoints()) {
            for(Point q : C2.getPoints()) {
                sum += Distance.calculateDistance(p.parseVector(), q.parseVector(), "standard");     
            }
        }
        return sum / (C1.getPoints().size() * C2.getPoints().size());      
    }
    
    public static double WardLinkage(Cluster C1, Cluster C2) {
        HashSet<Point> set = new HashSet(C1.getPoints());
        set.removeAll(C2.getPoints());
        ArrayList<Point> al = (ArrayList<Point>) C1.getPoints().clone();
        al.addAll(set);
        Cluster C12 = new Cluster(al);//dentro di se calcolo il centroide
        return C12.cost() - C1.cost() * C2.cost();
    }    
    
}
