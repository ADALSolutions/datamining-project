/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author DavideDP
 */
public class XMeans 
{
    public static void main(String args[]) throws IOException
    {
         testRemoveOutliers( args) ;
    }
        public static void testXMeans(String args[]) throws IOException
    {
        System.out.println("Test XMEANS");
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Workspace\\datamining-project");
        SparkConf sparkConf = new SparkConf(true).setAppName("Test XMEANS");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //LEGGO INPUT
        JavaRDD<Point> points = Utility.leggiInput("input.txt", sc);
        List<Point> coll = points.collect();
        ArrayList<Point> P=new ArrayList<Point>(coll.size());
        P.addAll(coll);
        int k=3;
        ArrayList<Point> S = ClusteringBuilder.kmeansPlusPlus(P, k);
        Clustering XMeans = XMeans(P,S,k,20);
        
    }
    public static void testRemoveOutliers(String args[]) throws IOException
    {
        System.out.println("Test XMEANS");
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Workspace\\datamining-project");
        SparkConf sparkConf = new SparkConf(true).setAppName("Test XMEANS");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //LEGGO INPUT
        JavaRDD<Point> points = Utility.leggiInput("input.txt", sc);
        List<Point> coll = points.collect();
        ArrayList<Point> P=new ArrayList<Point>(coll.size());
        P.addAll(coll);
        int k=3;
        Utility.ORC(3, 2.58, P);
    }
    
    public static Clustering XMeans(ArrayList<Point> P, ArrayList<Point> S, int k_init,int kmax) throws IOException
    {
        int k=k_init;
        int comp=0;
        Clustering clustering=null;
        while(k<kmax)
        {
            System.out.println("Eseguo:"+comp);
            comp++;          
            clustering = ClusteringBuilder.kmeansAlgorithm_old(P, S, k);
            Utility.writeOuptut("outputBeforeSplit"+String.valueOf(comp)+".txt", clustering);
            //Improve Structure
            ArrayList<Cluster> clustersOld = (ArrayList<Cluster>)clustering.getClusters().clone();
            for(Cluster cluster:clustersOld)
            {
                //Stima loglikelihood non va bene per insiemi troppo piccoli
                if(cluster.size()<25)  { continue;}
                
                double oldBIC = XMeans.BIC(cluster,clustering.getM());
                int kSplit=2;
                ArrayList<Point> SSplit = ClusteringBuilder.kmeansPlusPlus(cluster.getPoints(), kSplit);
                Clustering clusteringSplit = ClusteringBuilder.kmeansAlgorithm_old(cluster.getPoints(), SSplit, kSplit);
                double newBIC = XMeans.BIC(clusteringSplit);
                if(oldBIC<newBIC)
                {
                    clustering.getClusters().remove(cluster);
                    for(int i=0;i<clusteringSplit.getK();i++)
                    {
                        clustering.addCluster(clusteringSplit.getClusters().get(i));
                    }  
                }

            }
            Utility.writeOuptut("outputAfterSplit"+String.valueOf(comp)+".txt", clustering);
            System.out.println("k:"+k+"   clusteringGetK"+clustering.getK());
            if(k==clustering.getK()){System.out.println("ESCO"); break;}
            k=clustering.getK();
            S=clustering.getCentroids();
        }
        return clustering;
    }
    public static double BIC(int k, int n, int d, double distortion, int[] clusterSize)
    {
        double variance=distortion/(n-k);
        double L=0;
        for(int i=0;i<k;i++)
        {
            L+=loglikelihood(n,clusterSize[i],variance,k,d);
            
        }
       int numParameters = k + k * d;
      
       return L - 0.5 * numParameters * Math.log(n);
    }   
        public static double BIC(Cluster c,int d)
    {
        int k=1;
        int[] clusterSize=new int[1]; 
        clusterSize[0]=c.size();     
        return BIC(k,c.size(),d,c.kmeans(),clusterSize);
        
    }
    public static double BIC(Clustering c)
    {
        int k=c.getK();
        int[] clusterSize=new int[k]; 
        for(int i=0;i<k;i++){clusterSize[i]=c.getClusters().get(i).size();}
        return BIC(k,c.size(),c.getM(),c.kmeans(),clusterSize);
        
    }
    public static double BIC_old(Clustering c)
    {
        double variance=c.kmeans()/(c.size()-c.getK());
        double L=0;
        int k=c.getK();
        for(Cluster cluster:c.getClusters())
        {

            L+=loglikelihood(c.size(),cluster.size(),variance,c.getK(),c.getM());
            
        }
       int numParameters = k + k * c.getM();
      
       return L - 0.5 * numParameters * Math.log(c.size());
    }
    public static double loglikelihood(int n,int ni,double variance,int k,int d)
    {
        return -0.5*(ni*Math.log(2*Math.PI)+ni*d*Math.log(variance)+(ni-k))+ni*Math.log(ni)-ni*Math.log(n);
        
    }
    
}
