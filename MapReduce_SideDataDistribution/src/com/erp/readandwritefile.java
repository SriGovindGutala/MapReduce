package com.erp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class readandwritefile 
{

public static void main(String args[]) throws IOException
  {
	 // FileInputStream in = null;
	 // FileOutputStream out = null;
	  
		  //in = new FileInputStream("Name.txt");
		//  out = new FileOutputStream("Output.txt");
         
		    String[] temp = null;
			HashMap<String,Integer> namedata = new HashMap<String,Integer>();
			namedata.put("Govi", 1);

		        // The name of the file to open.
		        String fileName = "Name.txt";
		        String fileName2 = "Output.txt";
		        // This will reference one line at a time
		        String line = null;

		        try 
		        {
		            // FileReader reads text files in the default encoding.
		            FileReader fileReader = new FileReader(fileName);
		            FileWriter fileWriter = new FileWriter(fileName2);
		            
		            // Always wrap FileReader in BufferedReader.
		            BufferedReader bufferedReader = new BufferedReader(fileReader);
		            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			        
		            //Reading My file and printing it
		            HashMap<String,Integer> bitches = new HashMap<String,Integer>();
		   	        String cache [] = new String[10];
		   	        int j = 0;
		            while((line = bufferedReader.readLine()) != null) 
		            {
		               String Cache_Data [] = line.split(",");
			        	
			    		bitches.put(Cache_Data[0], Integer.parseInt(Cache_Data[1]));
                        j++;
		               // temp = line.split(",");
		               // bufferedWriter.write(line);
			           // bufferedWriter.newLine();
		            }   
                    for(int i=0;i<j;i++)
                    {
                    	System.out.println(bitches.get(""));
                    }
		             
	            	//System.out.println(temp.length+"jjjjj");
		            
		            // Note that write() does not automatically append a newline character.
		           // for(int i=0;i<temp.length;i++)
		            //{
			          //  bufferedWriter.write(temp[i]);
			            //bufferedWriter.newLine();	
		           // }
		    
		            // Always close files.
		            bufferedWriter.close();
		            bufferedReader.close();
		        }
		        catch(FileNotFoundException ex) 
		        {
		            System.out.println("Unable to open file '" +fileName+ "'");                
		        }
		        catch(IOException ex) 
		        {
		            System.out.println("Error reading file '"+ fileName + "'");                  
		            // Or we could just do this: 
		            // ex.printStackTrace();
		        }
         }
	    }