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

	public static void main(String[] args) throws FileNotFoundException{
		HashMap<Integer, ArrayList<Polygon>> pairs 
									= new HashMap<Integer, ArrayList<Polygon>>();
		
		//1. file read & construct (id, polygons) pairs
		File polygons_file = new File(args[0]);
	    Scanner sc = new Scanner(polygons_file);
	 
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
	    
	    
	    //2. file read & construct points list
	    File points_file = new File(args[1]);
	    Scanner scp = new Scanner(points_file);
	    ArrayList<Point> points = new ArrayList<Point>();
	    String[] xy;
	    while (scp.hasNextLine()){
	    	xy = scp.nextLine().split(" ");
	    	points.add(new Point(Double.valueOf(xy[0]), Double.valueOf(xy[1])));
	    }
	    scp.close();
	    
	    
	    //3. find id corresponding coordinates & file write
	    BufferedWriter out = null;
		try {
			out = new BufferedWriter(new FileWriter(args[2]));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	    boolean found = false;
	    for(int i=0; i<points.size(); i++){
	    	for(Map.Entry<Integer,ArrayList<Polygon>> entry : pairs.entrySet()){
	    		if(found) break;
	    		ArrayList<Polygon> polys = entry.getValue();
	    		
	    		for(int j=0; j<polys.size(); j++){
	    			if(polys.get(j).contains(points.get(i))){
	    				try {
							out.write(entry.getKey().toString());
							out.newLine();
						} catch (IOException e) {
							e.printStackTrace();
						}
	    			}
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
