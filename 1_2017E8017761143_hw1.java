import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*; 
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.log4j.*;

public class Hw1Grp1 {

    public static void main(String[] args) throws IOException, URISyntaxException, MasterNotRunningException, ZooKeeperConnectionException {

	/*get file path from input string*/
	String[] R_split = args[0].split("=");
        String R = "hdfs://localhost:9000" + R_split[1];
	String[] S_split = args[1].split("=");
	String S = "hdfs://localhost:9000" + S_split[1];

	/*get the key columns from input string */
	String[] join_key = args[2].split("R|=|S");
	final int R_join_key = Integer.parseInt(join_key[1]);
	final int S_join_key = Integer.parseInt(join_key[3]);

	/*get the result columns from input string*/
	String[] res_col = args[3].split(":|,");
	List R_res_col = new ArrayList();
	List S_res_col = new ArrayList();
	for(int i=1; i<res_col.length; i++){
	    if( res_col[i].charAt(0) == 'R' ){
		int sum=0;
		for(int j=1; j<res_col[i].length(); j++){
		    sum = sum *10 + ((int)res_col[i].charAt(j) -48);
		}
		R_res_col.add(sum);
		continue;}
	    if( res_col[i].charAt(0) == 'S' ){
		int sum=0;
		for(int j=1; j<res_col[i].length(); j++){
		    sum = sum *10 + ((int)res_col[i].charAt(j) -48);
		}
		S_res_col.add(sum);
		continue;}
	}
/*	for(int i=0; i<R_res_col.size(); i++){
		System.out.println("R:"+R_res_col.get(i));
	}
	for(int i=0; i<S_res_col.size(); i++){
		System.out.println("S:"+S_res_col.get(i));
	}*/
	

	/*get configuration file*/
        Configuration conf = new Configuration();
	/*get filesystem*/
        FileSystem fs = FileSystem.get(URI.create(R), conf);
	/*file path information for filesystem*/
        Path path = new Path(R);
	/*input stream of given-path file data*/
        FSDataInputStream in_stream = fs.open(path);
	/*read the data by line*/
        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));

	/*split and store the lines of R file in R_list*/
        String s;
	List R_list=new ArrayList();
        while ((s=in.readLine())!=null) {
	    String[] R_line_split=s.split("\\|");
	    R_list.add(R_line_split);
        }

	/*read the data from S file*/
        fs = FileSystem.get(URI.create(S), conf);
        path = new Path(S);
        in_stream = fs.open(path);
        in = new BufferedReader(new InputStreamReader(in_stream));
	/*split and store the lines of Sfile in S_list*/
//	int cnt=0;
	List S_list=new ArrayList();
        while ((s=in.readLine())!=null) {
	    String[] S_line_split=s.split("\\|");
	     S_list.add(S_line_split);
//		cnt+=1;
//		if(cnt>14980)
//		  System.out.println(s);
        }

	/*sort R_list by its join_key column*/
	Collections.sort(R_list,new Comparator<Object>(){
	     public int compare(Object o1, Object o2){
		String[] s1 = (String[]) o1;
		String[] s2 = (String[]) o2;
		String key_col_1 = s1[R_join_key];
		String key_col_2 = s2[R_join_key];
		int comparasion=key_col_1.compareTo(key_col_2);
		if( comparasion>0 ){
		   return 1;
		}
		if( comparasion<0 ){
		   return -1;
		}
		return 0;		
		}
	});
	/*sort S_list by it's join_key column*/
	Collections.sort(S_list,new Comparator<Object>(){
	     public int compare(Object o1, Object o2){
		String[] s1 = (String[]) o1;
		String[] s2 = (String[]) o2;
		String key_col_1 = s1[S_join_key];
		String key_col_2 = s2[S_join_key];
		int comparasion = key_col_1.compareTo(key_col_2);
//		System.out.println(comparasion);
		if( comparasion>0 ){
		   return 1;
		}
		if( comparasion<0 ){
		   return -1;
		}
		return 0;		
	     }
	});

        in.close();
        fs.close();
	
	Logger.getRootLogger().setLevel(Level.WARN);

	// create table descriptor
	String tableName= "Result";
	HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

	// create column descriptor
	HColumnDescriptor cf = new HColumnDescriptor("res");
	htd.addFamily(cf);

	// configure HBase
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);

        if (hAdmin.tableExists(tableName)) {
            System.out.println("Table already exists");
	    hAdmin.disableTable(tableName);  
            hAdmin.deleteTable(tableName);  
            System.out.println(tableName + " is exist,detele....");
        }
        hAdmin.createTable(htd);
        System.out.println("table "+tableName+ " created successfully");
	hAdmin.close();

	// put "mytable","abc","mycf:a","789"

	HTable table = new HTable(configuration,tableName);
	
	//merge and put the joining result in the table
	int i=0,j=0;
	String[] R_line_arr, S_line_arr;
	while( i<R_list.size() && j<S_list.size()){
	    R_line_arr = (String[])R_list.get(i);
	    S_line_arr = (String[])S_list.get(j);
	    int R_vs_S = R_line_arr[R_join_key].compareTo(S_line_arr[S_join_key]);
	    if( R_vs_S > 0 ){//the two join keys are not equal, then check the next join key
		j += 1;
		continue;
	    }else if( R_vs_S < 0 ){
		i += 1;
		continue;
	    }else{//the two join keys are equal, then execute joining and putting
		int i1 = i + 1, j1 = j + 1;
		while( i1 < R_list.size() ){
		    R_line_arr = (String[])R_list.get(i1);
	    	    S_line_arr = (String[])S_list.get(j);
	    	    R_vs_S = R_line_arr[R_join_key].compareTo(S_line_arr[S_join_key]);
	    	    if( R_vs_S == 0 ){//the two join keys are not equal, then check the next join key
			    i1 += 1;
		    }else{break;}
		}

		while( j1 < S_list.size() ){
		    R_line_arr = (String[])R_list.get(i);
	    	    S_line_arr = (String[])S_list.get(j1);
	    	    R_vs_S = R_line_arr[R_join_key].compareTo(S_line_arr[S_join_key]);
	    	    if( R_vs_S == 0 ){//the two join keys are not equal, then check the next join key
			    j1 += 1;
		    }else{break;}
		}

		Put put = new Put( R_line_arr[R_join_key].getBytes() );
		int add_cnt = 0;
		for( int m = i; m < i1; m++){
		    for(int n = j; n<j1; n++){
			if(add_cnt == 0){
			int res_col_cnt = 1;
			    for(int k=0; k<R_res_col.size(); k++){
		    	    	put.add("res".getBytes(),res_col[res_col_cnt].getBytes(),R_line_arr[(int)R_res_col.get(k)].getBytes());
		   	    	res_col_cnt += 1;
			    }
			    for(int k=0; k<S_res_col.size(); k++){
		    	    	put.add("res".getBytes(),res_col[res_col_cnt].getBytes(),S_line_arr[(int)S_res_col.get(k)].getBytes());
		    	    	res_col_cnt += 1;
			}}else{
		    	    String add_cnt_str = Integer.toString(add_cnt);
		    	    int res_col_cnt = 1;
		    	    for(int k=0; k<R_res_col.size(); k++){
		        	put.add("res".getBytes(),(res_col[res_col_cnt]+'.'+add_cnt_str).getBytes(),R_line_arr[(int)R_res_col.get(k)].getBytes());
		        	res_col_cnt += 1;
		    	    }
		    	    for(int k=0; k<S_res_col.size(); k++){
		        	put.add("res".getBytes(),(res_col[res_col_cnt]+'.'+add_cnt_str).getBytes(),S_line_arr[(int)S_res_col.get(k)].getBytes());
		        	res_col_cnt += 1;
		    	    }	
			}
		    	add_cnt += 1;
		    }
		}
		i = i1;
		j = j1;
		table.put(put);
	    }
	}
	table.close();
	System.out.println("put successfully");
	
	}
}

