package main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Scanner;

import main.Point;
import main.Polygon;
import main.Polygon.Builder;


public class Converter {

	public static void main(String[] args){
		
		/*for(int y=6; y<7; y++){
			for(int i=1; i<10; i++){
				run("..\\zipcode.txt",
					"..\\201"+y+"0"+i+"\\201"+y+"0"+i+"_citibike_startGPS.txt",
					"..\\bike\\201"+y+"0"+i+"_citibike_startZip.txt");
				run("..\\zipcode.txt",
					"..\\201"+y+"0"+i+"\\201"+y+"0"+i+"_citibike_endGPS.txt",
					"..\\bike\\201"+y+"0"+i+"_citibike_endZip.txt");
			}
			for(int i=11; i<13; i++){
				run("..\\zipcode.txt",
					"..\\201"+y+i+"\\201"+y+i+"_citibike_startGPS.txt",
					"..\\bike\\201"+y+i+"_citibike_startZip.txt");
				run("..\\zipcode.txt",
					"..\\201"+y+i+"\\201"+y+i+"_citibike_endGPS.txt",
					"..\\bike\\201"+y+i+"_citibike_endZip.txt");
			}
		}*/
		String date = "";
		String gps_path = "";
		for(int y=6; y<7; y++){
			for(int i=7; i<10; i++){
				date = "201"+y+"-0"+i;
				//gps_path = "..\\"+date+"\\"+date+"_taxi_startGPS.txt";
				gps_path = "..\\2016-07_taxi_startGPS.txt";
				run("..\\zipcode.txt",
						gps_path,
					"..\\taxi\\201"+y+"0"+i+"_taxi_startZip.txt");
				run("..\\zipcode.txt",
					"..\\"+date+"\\"+date+"_taxi_endGPS.txt",
					"..\\taxi\\201"+y+"0"+i+"_taxi_endZip.txt");
			}
			for(int i=11; i<13; i++){
				date = "201"+y+"-0"+i;
				run("..\\zipcode.txt",
					"..\\"+date+"\\"+date+"_taxi_startGPS.txt",
					"..\\taxi\\"+date+"_taxi_startZip.txt");
				run("..\\zipcode.txt",
					"..\\"+date+"\\"+date+"_taxi_endGPS.txt",
					"..\\taxi\\201"+y+i+"_taxi_endZip.txt");
			}
		}
		
	}
	public static void run(String poly_file, String in_file, String out_file){
		HashMap<Integer, ArrayList<Polygon>> pairs //HashMap<Zipcode, polygon_lis
									= new HashMap<Integer, ArrayList<Polygon>>();
		
		//1. file read & construct (id, polygons) pairs
		File polygons_file = new File(poly_file);
	    Scanner sc = null;
		try {
			sc = new Scanner(polygons_file);
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
		}
	 
	    while (sc.hasNextLine()){
	    	int id = sc.nextInt();
	    	int poly_cnt = sc.nextInt();
	    	ArrayList<Polygon> polys = new ArrayList<Polygon>();
	    	
	    	for(int i=0; i<poly_cnt; i++){//add all polygons of the ID
	    		Builder builder = Polygon.Builder();
	    		int point_cnt = sc.nextInt();
	    		sc.nextLine();
	    		String[] xy;
	    		
	    		for(int j=0; j<point_cnt; j++){//build a polygon with all points
	    			xy= sc.nextLine().split(" ");
	    			Point p = new Point(Double.valueOf(xy[0]), Double.valueOf(xy[1]));
	    			builder.addVertex(p);
	    		}
	    		Polygon poly = builder.build();
	    		polys.add(poly);
	    	}
	    	pairs.put(id, polys);
	    }
	    sc.close();
	    System.out.println(pairs.size());//paris.size(): the number of Zipcode
	    
	    
	    //2. file read & construct points list
	    File points_file = new File(in_file);
	    Scanner scp = null;
		try {
			scp = new Scanner(points_file);
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	    ArrayList<Point> points = new ArrayList<Point>();
	    String[] xy;
	    //scp.nextLine();//for citibike
	    while (scp.hasNextLine()){
	    	xy = scp.nextLine().split(" ");
	    	if ( xy[0].toString().equals("None") || xy[1].toString().equals("None"))
	    		points.add(new Point(Double.valueOf(0), Double.valueOf(0)));
	    	else
	    		points.add(new Point(Double.valueOf(xy[0]), Double.valueOf(xy[1])));
	    }
	    System.out.print(points.size());
	    scp.close();
	    
	    
	    //3. find id corresponding coordinates & file write
	    BufferedWriter out = null;
		try {
			out = new BufferedWriter(new FileWriter(out_file));
			out.write("Zipcode");
			out.newLine();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	    boolean found = false;
	    for(int i=0; i<points.size(); i++){//For each point
	    	found = false;
	    	for(Map.Entry<Integer,ArrayList<Polygon>> entry : pairs.entrySet()){//For each polygon
	    		if(found)	break;
	    		ArrayList<Polygon> polys = entry.getValue();
	    		
	    		for(int j=0; j<polys.size(); j++){//For a case of having several polygons per a Zipcode 
	    			if(polys.get(j).contains(points.get(i))){
	    				try {
							out.write(entry.getKey().toString());
							out.newLine();
							found = true;
							break;
						} catch (IOException e) {
							e.printStackTrace();
						}
	    			}
	    		}
	    	}
	    	if(!found){
				try {
					out.write("None");
					out.newLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
	    	}
	    }
	    try {
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
