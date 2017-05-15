package it.unipd.dei.dm1617;


import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;

public class Distance implements Serializable
{


  /**
   * Cosine distance between vectors where all the elements are positive.
   */
  public static double cosineDistance(Vector a, Vector b) {
    if (a.size() != b.size()) {
      throw new IllegalArgumentException("Vectors should be in the same space");
    }
    
    double num = 0;
    for (int i=0; i<a.size(); i++) {
      num += a.apply(i) * b.apply(i);
    }//BLAS.axpy()
    double normA = Vectors.norm(a, 2);
    double normB = Vectors.norm(b, 2);

    double cosine = num / (normA * normB);
    if (cosine > 1.0) {
      // Mathematically, this should't be possible, but due to the
      // propagation of errors in floating point operations, it
      // happens
      cosine = 1;
    }
    // If you wish to use this function with vectors that can have
    // negative components (like the ones given by word2vec), then
    // rescale by PI instead of PI/2
    return (2 / Math.PI) * Math.acos(cosine);
  }
public static double cosineDistance2(Vector a, Vector b) 
{
    if (a.size() != b.size()) {
      throw new IllegalArgumentException("Vectors should be in the same space");
    }
    Vector c=a.copy();
    double dot = BLAS.dot( b, c);
    double den=Vectors.norm(a,2)* Vectors.norm(b,2);
    return Math.acos( dot/den);
}
  public static double euclideanDistance(Vector a, Vector b,int r)
  {
    if (a.size() != b.size()) {
      throw new IllegalArgumentException("Vectors should be in the same space");
    }
      Vector c = b.copy();  //b
      BLAS.scal(-1,c);     //-b
      BLAS.axpy(1, a, c);  //a-b
      return Vectors.norm(c, r);//ritorno norma
  }
  public static double euclideanDistance2(Vector a, Vector b,int r)
  {
    if (a.size() != b.size()) {
      throw new IllegalArgumentException("Vectors should be in the same space");
    }
      double sum=0;
      for(int i=0;i<a.size();i++)
      {
         sum+=Math.pow(Math.abs(a.apply(i)-b.apply(i)),r);
      }
      
      return Math.pow(sum,((double)1)/r);
  } 
  
  public static double standardDistance(Vector a, Vector b)
  {
      return euclideanDistance(a,b,2);
  }
  public static double manhattanDistance(Vector a, Vector b)
  {
      return euclideanDistance(a,b,1);
  } 
  public static double jaccardDistance(Vector a, Vector b)
  {
          if (a.size() != b.size()) {
      throw new IllegalArgumentException("Vectors should be in the same space");
    }
    /*
    HashSet s = new HashSet(Clustering.toVector(a.toArray()));
    HashSet t = new HashSet(Clustering.toVector(b.toArray()));
    */     
    HashSet s = new HashSet();
    HashSet t = new HashSet();
      for(int i=0;i<a.size();i++)
      {
        s.add(a.apply(i));
        t.add(b.apply(i));
      }     
      HashSet union= (HashSet) s.clone();
      union.addAll(t);
      HashSet intersect= (HashSet) s.clone();
      intersect.remove(t);
      return 1-((double)union.size())/intersect.size();  
  } 
  public static double hammingDistance(Vector a, Vector b)
  {
          if (a.size() != b.size()) {
      throw new IllegalArgumentException("Vectors should be in the same space");
    }
      int cont=0;
      for(int i=0;i<a.size();i++)
      {
          if(a.apply(i)==b.apply(i))cont++;
      }
      return cont;
  }
  /*
  Used for strings. Given two strings X and Y ,
their edit distance is the minimum number of deletions of
insertions that must be applied to transform X into Y .
  |X| + |Y | − 2|LCS(X, Y )|
  */
  public static double editDistance(Vector a, Vector b)
  {
      //ammetto lunghezze diverse ?
      //ipotizzo che il double sia equivalente al char
      StringBuilder sa=new StringBuilder();
      StringBuilder sb=new StringBuilder();
       for(int i=0;i<a.size();i++)
      {
          sa.append((char)a.apply(i));
          sb.append((char)b.apply(i));
      }  
      String lcs = LCS.LCSAlgorithm(sa.toString(), sb.toString());
      return a.size()+b.size()-2*lcs.length();
  }
  public static double upperBound(Vector a, Vector b)
  {
    if (a.size() != b.size()) {
      throw new IllegalArgumentException("Vectors should be in the same space");
    }
      return 2*Math.sqrt(a.size());
  }
  public static double calculateDistance(Vector a, Vector b)
  {
      return Distance.standardDistance(a,b);
  }
    
}
