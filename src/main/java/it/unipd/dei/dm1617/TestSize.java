/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import static it.unipd.dei.dm1617.Lemmatizer.lemmatize;
import java.util.ArrayList;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

/**
 *
 * @author DavideDP
 */
public class TestSize

{
  public static void main(String[] args) {
    double p[]={1,2,5,6,7,8,9,3,5,6,9,6,12,53,56,3456,754,345,8765,456};
    //for(int i=0;)
    System.out.println("dimensione : "+p.length);
    System.out.println("Size dell'array double : "+ObjectSizeCalculator.getObjectSize(p));
    ArrayList<Double> a=new ArrayList<Double>();
    for(int i=0;i<p.length;i++){a.add((double)p[i]);}
    PointCentroid point = new PointCentroid(a);
    System.out.println("Size del Point: "+ObjectSizeCalculator.getObjectSize(point));
    System.out.println("Size del Vector di spark : "+ObjectSizeCalculator.getObjectSize(point.parseVector()));
    System.out.println("Size dell' ArrayList : "+ObjectSizeCalculator.getObjectSize(point.point));
  }    
    
    

    
}
