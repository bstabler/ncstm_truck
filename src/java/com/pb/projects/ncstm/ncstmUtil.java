package com.pb.projects.ncstm;

import com.pb.common.datafile.CSVFileReader;
import com.pb.common.datafile.TableDataSet;
import com.pb.common.matrix.MatrixType;
import com.pb.common.matrix.MatrixWriter;
import com.pb.common.util.ResourceUtil;
import org.apache.log4j.Logger;
import java.io.*;
import java.util.ResourceBundle;

/**
 * Utilities for model to simulate truck flows for the North Carolina Statewide Model (NCSTM)
 * Author: Rolf Moeckel, PB Albuquerque
 * Data: 29 July 2011
 */
public class ncstmUtil {

    static Logger logger = Logger.getLogger(ncstmUtil.class);


    public static PrintWriter openFileForSequentialWriting(String fileName) {
        File outputFile = new File(fileName);
        FileWriter fw = null;
        try {
            fw = new FileWriter(outputFile);
        } catch (IOException e) {
            logger.error("Could not open file <" + fileName + ">.");
        }
        BufferedWriter bw = new BufferedWriter(fw);
        return new PrintWriter(bw);
    }


    public static ResourceBundle getResourceBundle(String pathToRb) {
        File propFile = new File(pathToRb);
        ResourceBundle rb = ResourceUtil.getPropertyBundle(propFile);
        if (rb == null) logger.fatal ("Problem loading resource bundle: " + pathToRb);
        return rb;
    }


    public static TableDataSet importTable(String filePath) {
        // read a csv file into a TableDataSet
        TableDataSet tblData;
        CSVFileReader cfrReader = new CSVFileReader();
        try {
            tblData = cfrReader.readFile(new File( filePath ));
        } catch (Exception e) {
            throw new RuntimeException("File not found: <" + filePath + ">.", e);
        }
        cfrReader.close();
        return tblData;
    }


    public static float rounder(float value, int digits) {
        // rounds value to digits behind the decimal point
        return Math.round(value * Math.pow(10, digits) + 0.5)/(float) Math.pow(10, digits);
    }


    public static float rounder(double value, int digits) {
        // rounds value to digits behind the decimal point
        return Math.round(value * Math.pow(10, digits) + 0.5)/(float) Math.pow(10, digits);
    }


	public static MatrixWriter createTransCADWriter(ResourceBundle rb, String fileName){
        // create Matrix writer for TransCAD
		MatrixWriter writer;
		writer = MatrixWriter.createWriter(MatrixType.TRANSCAD, new File(fileName));			
		return writer;		
	}


    public static int getHighestVal(int[] array) {
        // return highest number in array

        int high = -999999999;
        for (int num: array) high = Math.max(high, num);
        return high;
    }


    public static double getSum(double[] array) {
        double sm = 0;
        for (double d: array) sm += d;
        return sm;
    }


    public static double getSum(double[][] array) {
        // sum values of array[][]
        double sm = 0;
        for (double[] anArray : array) {
            for (int j = 0; j < array[0].length; j++) {
                sm += anArray[j];
            }
        }
        return sm;
    }


    public static int[] sortNumbers (int[] array) {

        int[] index = new int[array.length];
        for(int i=0;i<array.length-1;i++)
        {
            for(int j=i+1;j<array.length;j++)
            {
                if(array[i]>array[j])
                {
                    index[i]=j;
                    index[j]=i;
                }
            }
        }
        return index;
    }
}
