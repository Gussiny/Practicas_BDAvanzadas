package tarea3;

import java.sql.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class tarea3 {
	public static void main(String[] args) {
		try {
	        Configuration conf = HBaseConfiguration.create();
	        Connection cn = ConnectionFactory.createConnection(conf);
	        Admin hbaseAdmin = cn.getAdmin();
	
	        
	        HTableDescriptor tabla = new HTableDescriptor(TableName.valueOf("tocho"));
	        tabla.addFamily(new HColumnDescriptor("id"));
	        tabla.addFamily(new HColumnDescriptor("nombre"));
	        tabla.addFamily(new HColumnDescriptor("apellido"));
	        tabla.addFamily(new HColumnDescriptor("edad"));
	        tabla.addFamily(new HColumnDescriptor("posicion"));
	
	        if (hbaseAdmin.tableExists(TableName.valueOf("tocho"))) {
	            System.out.println("TABLA YA CREADA");
	        } else {
	            hbaseAdmin.createTable(tabla);
	            System.out.println("TABLA CREADA");
	        }
	        
	        java.sql.Connection myConn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/db", "root", "Puto-123");
	
	        Statement myStmt = myConn.createStatement();
	
	        ResultSet myRs = myStmt.executeQuery("SELECT * FROM tocho;");
	
	        Table table = cn.getTable(TableName.valueOf("tocho"));
	
	        while (myRs.next()) {
	            Put p = new Put(Bytes.toBytes(""+myRs.getInt("id")));
	
	            p.addColumn(Bytes.toBytes("nombre"),Bytes.toBytes("Nombre"),Bytes.toBytes(myRs.getString("nombre")));
	            p.addColumn(Bytes.toBytes("apellido"),Bytes.toBytes("Apellido"),Bytes.toBytes(myRs.getString("apellido")));
	            p.addColumn(Bytes.toBytes("edad"),Bytes.toBytes("Edad"),Bytes.toBytes(myRs.getString("edad")));	         
	            p.addColumn(Bytes.toBytes("posicion"),Bytes.toBytes("Posicion"),Bytes.toBytes(myRs.getString("posicion")));
	            table.put(p);
	        }
	        
	        System.out.println("DATOS INSERTADOS");
	        
	        Table t = cn.getTable(TableName.valueOf("tocho"));
	        
	        //	QB
			SingleColumnValueFilter f=new SingleColumnValueFilter(Bytes.toBytes("posicion"),Bytes.toBytes("Posicion"),CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes("QB")));
	        Scan sc = new Scan();
	        System.out.println("QB");
	        sc.setFilter(f);
	        ResultScanner rs = t.getScanner(sc);
	        for(Result r1:rs) {
				byte[] nombre = r1.getValue(Bytes.toBytes("nombre"), Bytes.toBytes("Nombre"));
				byte[] dorsal = r1.getRow();
				byte[] apellido = r1.getValue(Bytes.toBytes("apellido"), Bytes.toBytes("Apellido"));
				System.out.println("Nombre: " + Bytes.toString(nombre) + " " + Bytes.toString(apellido) + " Dorsal:" + Bytes.toString(dorsal));
			}
			
	        //	RB
			f=new SingleColumnValueFilter(Bytes.toBytes("posicion"),Bytes.toBytes("Posicion"),CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes("RB")));
		    sc = new Scan();
		    System.out.println("RB");
		    sc.setFilter(f);
		    rs = t.getScanner(sc);
			for(Result r1:rs) {
				byte[] nombre = r1.getValue(Bytes.toBytes("nombre"), Bytes.toBytes("Nombre"));
				byte[] dorsal = r1.getRow();
				byte[] apellido = r1.getValue(Bytes.toBytes("apellido"), Bytes.toBytes("Apellido"));
				System.out.println("Nombre: " + Bytes.toString(nombre) + " " + Bytes.toString(apellido) + " Dorsal:" + Bytes.toString(dorsal));
			}

			//	WR
			f=new SingleColumnValueFilter(Bytes.toBytes("posicion"),Bytes.toBytes("Posicion"),CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes("WR")));
		    sc = new Scan();
		    System.out.println("WR");
		    sc.setFilter(f);
		    rs = t.getScanner(sc);
			for(Result r1:rs) {
				byte[] nombre = r1.getValue(Bytes.toBytes("nombre"), Bytes.toBytes("Nombre"));
				byte[] dorsal = r1.getRow();
				byte[] apellido = r1.getValue(Bytes.toBytes("apellido"), Bytes.toBytes("Apellido"));
				System.out.println("Nombre: " + Bytes.toString(nombre) + " " + Bytes.toString(apellido) + " Dorsal:" + Bytes.toString(dorsal));
			}
			
			//	TE
			f=new SingleColumnValueFilter(Bytes.toBytes("posicion"),Bytes.toBytes("Posicion"),CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes("TE")));
		    sc = new Scan();
		    System.out.println("TE");
		    sc.setFilter(f);
		    rs = t.getScanner(sc);
			for(Result r1:rs) {
				byte[] nombre = r1.getValue(Bytes.toBytes("nombre"), Bytes.toBytes("Nombre"));
				byte[] dorsal = r1.getRow();
				byte[] apellido = r1.getValue(Bytes.toBytes("apellido"), Bytes.toBytes("Apellido"));
				System.out.println("Nombre: " + Bytes.toString(nombre) + " " + Bytes.toString(apellido) + " Dorsal:" + Bytes.toString(dorsal));
			}

			
			//	C
			f=new SingleColumnValueFilter(Bytes.toBytes("posicion"),Bytes.toBytes("Posicion"),CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes("C")));
		    sc = new Scan();
		    System.out.println("C");
			sc.setFilter(f);
			rs = t.getScanner(sc);
			for(Result r1:rs) {
				byte[] nombre = r1.getValue(Bytes.toBytes("nombre"), Bytes.toBytes("Nombre"));
				byte[] dorsal = r1.getRow();
				byte[] apellido = r1.getValue(Bytes.toBytes("apellido"), Bytes.toBytes("Apellido"));
				System.out.println("Nombre: " + Bytes.toString(nombre) + " " + Bytes.toString(apellido) + " Dorsal:" + Bytes.toString(dorsal));
			}
			
			//	CB
			f=new SingleColumnValueFilter(Bytes.toBytes("posicion"),Bytes.toBytes("Posicion"),CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes("CB")));
			sc = new Scan();
			System.out.println("CB");
			sc.setFilter(f);
			rs = t.getScanner(sc);
			for(Result r1:rs) {
				byte[] nombre = r1.getValue(Bytes.toBytes("nombre"), Bytes.toBytes("Nombre"));
				byte[] dorsal = r1.getRow();
				byte[] apellido = r1.getValue(Bytes.toBytes("apellido"), Bytes.toBytes("Apellido"));
				System.out.println("Nombre: " + Bytes.toString(nombre) + " " + Bytes.toString(apellido) + " Dorsal:" + Bytes.toString(dorsal));
			}
			
			//	LB
			f=new SingleColumnValueFilter(Bytes.toBytes("posicion"),Bytes.toBytes("Posicion"),CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes("LB")));
			sc = new Scan();
			System.out.println("LB");
			sc.setFilter(f);
			rs = t.getScanner(sc);
			for(Result r1:rs) {
				byte[] nombre = r1.getValue(Bytes.toBytes("nombre"), Bytes.toBytes("Nombre"));
				byte[] dorsal = r1.getRow();
				byte[] apellido = r1.getValue(Bytes.toBytes("apellido"), Bytes.toBytes("Apellido"));
				System.out.println("Nombre: " + Bytes.toString(nombre) + " " + Bytes.toString(apellido) + " Dorsal:" + Bytes.toString(dorsal));
			}
			
			
	        cn.close();
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}
}
