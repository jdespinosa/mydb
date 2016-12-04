/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mydb;

/**
 *
 * @author JDE
 * @description This tests the many functionalities of this library
 */
public class DBTest {
    private final String dburl;
    private final String schemaurl;
    
    public DBTest(){
        this.dburl = "c:/mydb/db1";
        this.schemaurl = "c:/mydb/master.schema";
    }
    
    public void createTable(String tablename){
        Database db = new Database(dburl, schemaurl);
        Field[] fields = new Field[4];
        
        fields[0] = new Field("name", "VARCHAR", 35 );
        fields[1] = new Field("course", "VARCHAR", 30);
        fields[2] = new Field("year", "INT", 0);
        fields[3] = new Field("id", "VARCHAR", 10);
        
        db.createTable(tablename, fields);
    }
    
    public void accessRecords(){
        Database db = new Database(dburl, schemaurl);
        String tablename = "student";
        Table table = db.getTable(tablename);
        
        Field[] fields = new Field[2];
        
        fields[0] = table.getField(0);
        fields[1] = table.getField(1);
        
        Recordset rs = db.getRecords(table, fields, null);
        
        //System.out.println(rs);
        
        
        while(rs.hasNext()){
            Record rec = rs.nextRecord();
            System.out.println(rec.getFieldValue(fields[0].getName()));
            System.out.println(rec.getFieldValue(fields[1].getName()));
            
        }
        
        
    }
    
    
    
    public void insertRecord(){
        Database db = new Database(dburl, schemaurl);
        
        DataValue[] dv = new DataValue[4];
        dv[0] = new DataValue("name", "June Dick Espinosa");
        dv[1] = new DataValue("course", "Software Engineering");
        dv[2] = new DataValue("year", 5);
        dv[3] = new DataValue("id", "001-11");
        
        
        db.insertToTable("student", dv);
        
        dv[0] = new DataValue("name", "Mark Dominique Espinosa");
        dv[1] = new DataValue("course", "Nursing");
        dv[2] = new DataValue("year", 5);
        dv[3] = new DataValue("id", "002-22");
        
        db.insertToTable("student", dv);
    }
    
    public void updateRecords(){
        Database db = new Database(dburl, schemaurl);
        Table table = db.getTable("student");
        
        // create DataValues (assuming you just want to update 2 fields
        DataValue[] dv = new DataValue[2];
        dv[0] = new DataValue("year", 1);
        dv[1] = new DataValue("course", "ECE");
        
        // Field to be included in your filter (e.g. WHERE id="001-11" OR id="002-22"
        Field fage = table.getField("id");
        
        FilterExpression fexpr1 = new FilterExpression(new FilterTerm(fage), DBRelationalOptr.EQUAL, new FilterTerm("002-22"), null, null);
        FilterExpression fexpr2 = new FilterExpression(new FilterTerm(fage), DBRelationalOptr.EQUAL, new FilterTerm("001-11"), DBLogicalOptr.OR, fexpr1);
        
        db.updateTable("student", dv, fexpr2);
    }
    
    public static void main(String[] args){
        DBTest dbt = new DBTest();
        
        //create student table
        // dbt.createTable("student");
        
        // insert a record to student table
        // dbt.insertRecord();
        
        // dbt.updateRecords();
        dbt.accessRecords();
    }
    
}
