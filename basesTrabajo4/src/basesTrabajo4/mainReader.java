package basesTrabajo4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class mainReader {
	
	
	public static void addData(Connection con) {
		File file = new File("src/basesTrabajo4/heroes_information.csv");
		String absolutePath = file.getAbsolutePath();
		
		System.out.println(absolutePath);
		//String pathToCsv = "/basesTrabajo4/src/basesTrabajo4/heroes_information.csv";
		String pathToCsv = absolutePath;
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
				addData(con);
			}
			
			menuDeFiltros(con);
			con.close();
			
		}catch(Exception e) {
			System.out.println("Error "+e);
		}
			
	}
	
	public static void menuDeFiltros(Connection con) {
		System.out.println(""
				+ "QUE DESEA BUSCAR?\n"
				+ "1) MARVEL\n"
				+ "2) DC COMICS\n"
				+ "3) DARK HORSE COMICS\n"
				+ "4) IMAGE COMICS");
		
		Scanner sc = new Scanner(System.in);
		String opcPublisher = sc.nextLine();

		String publisher = "";
		
		if(opcPublisher.equals("1")) {
			publisher = "Marvel Comics";
		}else if(opcPublisher.equals("2")) {
			publisher = "DC Comics";
		}else if(opcPublisher.equals("3")) {
			publisher = "Dark Horse Comics";
		}else if(opcPublisher.equals("4")) {
			publisher = "Image Comics";
		}
		
		
		System.out.println(""
				+ "QUE BANDO QUIERES MOSTRAR DE " + publisher + "\n"
				+ "1) BUENOS\n"
				+ "2) MALOS\n"
				+ "3) NEUTRALES\n"
				+ "4) OTROS");
		
		Scanner sc2 = new Scanner(System.in);
		String opcAligment = sc2.nextLine();
		
		String aligment = "";
		
		if(opcAligment.equals("1")) {
			aligment = "good";
		}else if(opcAligment.equals("2")) {
			aligment = "bad";
		}else if(opcAligment.equals("3")) {
			aligment = "neutral";
		}else if(opcAligment.equals("4")) {
			aligment = "-";
		} 
		
		System.out.println("MOSTRANDO " + publisher + " - " + aligment);

		try {
	        Table t = con.getTable(TableName.valueOf("Superheroes"));
	        
			SingleColumnValueFilter f = new SingleColumnValueFilter(
					Bytes.toBytes(aligment),
					Bytes.toBytes("Publisher"),
					CompareOperator.EQUAL,
					new BinaryComparator(Bytes.toBytes(publisher)));
	        Scan scaneo = new Scan();
	        f.setFilterIfMissing(true);
	        scaneo.setFilter(f);
	        ResultScanner rs = t.getScanner(scaneo);
			
	        for(Result r1:rs) {
				byte[] name = r1.getValue(Bytes.toBytes(aligment), Bytes.toBytes("name"));
				byte[] id = r1.getRow();
				byte[] race = r1.getValue(Bytes.toBytes(aligment), Bytes.toBytes("Race"));
				System.out.println(" | ID:" + Bytes.toString(id) + " | Name: " + Bytes.toString(name) + " | Race: " + Bytes.toString(race) );
		}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
