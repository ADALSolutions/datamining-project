/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617.test;

import it.unipd.dei.dm1617.ClusteringBuilderMR;
import it.unipd.dei.dm1617.Point;
import it.unipd.dei.dm1617.PointCentroid;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author DavideDP
 */
public class TestKMeansNuovo 
{
    public static void main(String[] args) throws FileNotFoundException, IOException
    {
        System.out.println("MyFirstTest");
        System.setProperty("hadoop.home.dir", "C:\\Users\\DavideDP\\Desktop\\ProjectDM\\Workspace\\datamining-project");
        SparkConf sparkConf = new SparkConf(true).setAppName("Compute primes");
        JavaSparkContext sc = new JavaSparkContext(sparkConf); 
        //GESTIONE INPUT IN MANIERA PARALLELA
        JavaRDD<String> textFile = sc.textFile("input.txt");
        JavaRDD<Point> points=textFile.map(
        (doc)->
        {
            String[] split = doc.split("   "); 
            ArrayList<Double> al=new ArrayList<Double>();
            for(String ss:split)
            {
                if(ss.length()!=0)al.add(Double.parseDouble(ss));
            }
            return new PointCentroid(al);        
        }
        );
        
        //STAMPO INFO
        int k=6;
        System.out.println("Total points : "+points.count());
        System.out.println("Total clusters : "+k);
        
        ClusteringBuilderMR.kmeansAlgorithmMR(sc, points, SList, k)
    }
    
}
