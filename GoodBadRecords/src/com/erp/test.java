package com.erp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
public class test {

    public static void main(String args[])
    {
      boolean res,good,bad;
      String value = "Sujit,sdd,30,m"; 
      bad = false;
      good = false;
     
      ArrayList<String> data = new ArrayList<String>();
      StringTokenizer itr = new StringTokenizer(value.toString(),",");
      while (itr.hasMoreTokens()) 
      {
    	data.add(itr.nextToken());
      }
   // if there is no data in the array, it's a bad array
      for(int i=0;i<data.size();i++)
      {
          //System.out.println(data.get(i));
       if(i==1)
       {
   	      // if the second column is not integer, it's a bad array
 	      res = checkstring(data.get(i));
 	     System.out.println(res);
 	      if(res == true)
 	      {
 	    	  good = true;
 	      } 
 	      else
 	      {
 	    	  bad = true;
 	      }   
       }
       else
       {
         if(data.get(i).isEmpty())
         {
        	bad = true;
         }
         else
         {
        	good = true; 
         }
       }
      } 
      if (bad == true)
      {	        	
    	    System.out.println("bad");
      }
      else
      {    	 
    	    System.out.println("Good");
      }
    }

public static boolean checkstring(String s) 
{	
   boolean isInt = false;
   //System.out.println(s);
   try 
   { 
      Integer.parseInt(s);
      isInt = true;
   } 
   catch (NumberFormatException e) 
   {
     isInt = false;
   }
  return isInt;
}
}
