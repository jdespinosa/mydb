/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mydb;

/**
 *
 * @author JDE
 */
import java.io.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


public class Table {
    private Field[] fields;
    private String name;
    private String tableurl;
    private Schema schema;
    
    Table(String turl, Schema schema){
        this.tableurl = turl;
        this.schema = schema;
        try{
            File ftable = new File(turl);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(ftable);
            doc.getDocumentElement().normalize();
            
            // test whether this is a table schema
            Element e = doc.getDocumentElement();
            
            if( !e.getNodeName().equals("table") ){
                throw new Error("Invalid table schema file with url " + this.tableurl);
            }
            
            // get table name
            if( !e.hasAttribute("name") ){
                throw new Error("Invalid table name schema for ulr " + this.tableurl);
            }
            
            // get table name
            String name = e.getAttribute("name");
            this.name = name;
            
            if( name.equals("")  ){
                throw new Error("Invalid table name for url " + this.tableurl);
            }

            
            // attempt to get table fields
            NodeList nList = doc.getElementsByTagName("field");

            // check if field is at least one
            if( nList.getLength() < 1 ) {
                throw new Error("Invalid table. Must contain at least 1 field");
            }
            
            // initialize size of Data
            this.fields = new Field[nList.getLength()];

            for(int i=0; i<nList.getLength(); i++){
                Node node = nList.item(i);

                if( node.getNodeType() == Node.ELEMENT_NODE ){
                    e = (Element) node;
                    
                    String fldname = e.getAttribute("name");
                    String fldtype = e.getAttribute("type");
                    int fldlen;
                    
                    if( fldname.equals("") ){
                        throw new Error("Invalid table. Field name is invalid.");
                    }
                    
                    if( !schema.hasData(fldtype)  ){
                        throw new Error("Invalid table. Table contains invalid field data type");
                    }
                    
                    if( !e.hasAttribute("length")  ){
                        fldlen = 0;
                    }
                    else {
                        String s = e.getAttribute("length");
                        
                        try{
                            fldlen = Integer.parseInt(s);
                        }
                        catch(Exception ex){
                            fldlen = 0;
                        }
                        
                    }
                    
                    
                    this.fields[i] = new Field(fldname, fldtype, fldlen);
                }
            }
        }
        catch(Exception e) {
            throw new Error("Error accessing table schema file." + this.tableurl);
        }
        
    }
    
    public String getName(){
        return this.name;
    }
    
    public String[] getFieldNames(){
        String[] flds = new String[this.fields.length];
        
        for(int i=0; i<this.fields.length; i++){
           flds[i] = this.fields[i].getName();
        }
        
        return flds;
    }
    
    public Field getField(int index){
        if( index < 0 || index > this.fields.length ){
            return null;
        }
        
        return this.fields[index];
    }
    
    public Field getField(String name){
        int index = getFieldIndex(name);
        
        if( index == -1 ){
            return null;
        }
        
        return this.fields[index];
    }
    
    private int getFieldIndex( String name ){
        for(int i=0; i<fields.length; i++) {
            if( fields[i].getName().equals(name) ){
                return i;
            }
        }
        
        return -1;
    }
    
   public int getTableWidth(){
       int total =0;
       
       for( Field f : this.fields ){
           System.out.println(f.getName() + " " + getFieldWidth(f));
            total += getFieldWidth(f);
       }
       
       return total;
   }
   
   public int getFieldWidth(Field field){
       Data d = schema.getData(field.getDatatype());
       
       if( d.getLength() != -1   ){
           return d.getLength();
       }
       else {
           return field.getLength();   // for VARCHAR, base length NOT on DATA TYPE
       }
   }
    
    public static void main(String[] args){
        Schema schema = new Schema("c:/mydb/master.schema");
        Table t = new Table("c:/mydb/db1/student.tbl", schema);
        
        System.out.println("Table name: " + t.getName());
        System.out.println("Fields: ");
        String[] flds = t.getFieldNames();
        
        for(int i=0; i<flds.length; i++) {
            System.out.println("\tName: " + flds[i]);
            
            // field info
            Field f = t.getField(flds[i]);
            
            System.out.println("\tType: " + f.getDatatype());
            System.out.println("\tLength: " + f.getLength());
            System.out.println();
            
        }
        
       System.out.println("Table width: " + t.getTableWidth());
    }
    
}
