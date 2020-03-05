package basesTrabajo4;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class mainReader {
	
	
	public static void addData(Connection con) {
		String pathToCsv = "/home/pascal/Downloads/heroes_information.csv";
		BufferedReader csvReader=null;
		String [] attributes = {"id", "name","Gender","Eye color","Race","Hair color","Height","Publisher","Skin color","Alignment","Weight"}; 
		try {
			
			csvReader = new BufferedReader(new FileReader(pathToCsv));
			String line= csvReader.readLine();
			System.out.println(line);
			while ((line = csvReader.readLine()) != null) {
                String[] country = line.split(",");
                Put p=null;
                p= new Put(Bytes.toBytes(country[0]));
                for(int i=0;i<country.length;i++) {
                	System.out.println(attributes[i] +" -> "+country[i]);
        			p.addColumn(Bytes.toBytes(country[9]), Bytes.toBytes(attributes[i]), Bytes.toBytes(country[i]));
                } 
                Table t = con.getTable(TableName.valueOf("Superheroes"));
 
    			t.put(p);
                System.out.println("--------------------------------------------------");

            	
                

            }
		}catch(Exception e) {
			System.out.println("Error "+e);
		}finally {
			if (csvReader != null) {
                try {
                	csvReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
		}
	}
	
	
	
	public static void main(String[] args)  {
		try {
			Configuration conf = HBaseConfiguration.create();
			Connection con = ConnectionFactory.createConnection(conf);
			Admin hbaseAdmin = con.getAdmin();
			
			HTableDescriptor tabla = new HTableDescriptor(TableName.valueOf("Superheroes"));
			tabla.addFamily(new HColumnDescriptor("good"));
			tabla.addFamily(new HColumnDescriptor("bad"));
			tabla.addFamily(new HColumnDescriptor("neutral"));
			tabla.addFamily(new HColumnDescriptor("-"));

			
			
			if(hbaseAdmin.tableExists(TableName.valueOf("Superheroes"))){
				System.out.println("NO SE PUEDE CREAR PORQUE YA EXISTE PAPUUUUU");
			}else {
				hbaseAdmin.createTable(tabla);	
			}
			
			addData(con);
			
			con.close();
			
			
			
			
		}catch(Exception e) {
			System.out.println("Error "+e);
		}
		
		
		
		
	}

}
