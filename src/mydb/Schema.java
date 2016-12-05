/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mydb;

import java.io.*;
import org.w3c.dom.*;
import javax.xml.parsers.*;

public class Schema {
    protected Data[] data;
    private final String schema_url;
    private String spacer;
    
    Schema(String schema_url){
        this.schema_url = schema_url;
        
        try{
            File fschema = new File(this.schema_url);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fschema);
            doc.getDocumentElement().normalize();
            
            // test whether this is a master schema
            Element e = doc.getDocumentElement();
            
            if(!e.getNodeName().equals("schema")  || !e.getAttribute("type").equals("master")){
                throw new Error("Not valid schema master!");
            }
            
            NodeList nList = doc.getElementsByTagName("data");

            // initialize size of Data
            this.data = new Data[nList.getLength()];

            for(int i=0; i<nList.getLength(); i++){
                Node node = nList.item(i);

                if( node.getNodeType() == Node.ELEMENT_NODE ){
                    e = (Element) node;

                    String name = "";
                    int len = -1;
                    int maxlen = -1;

                    if( e.hasAttribute("name") ){
                        name = e.getAttribute("name");
                    }

                    if( e.hasAttribute("length") ) {
                        len = Integer.parseInt(e.getAttribute("length"));
                    }

                    if( e.hasAttribute("maxlength") ) {
                        maxlen = Integer.parseInt( e.getAttribute("maxlength"));
                    }

                    this.data[i] = new Data( name, len, maxlen);
                }
            }


            // read spacer string
            this.spacer = "#";     // default spacer value
            nList = doc.getElementsByTagName("spacer");

            if( nList.getLength() > 1 ){
                Node node = nList.item(0);
                e = (Element) node;
                if( e.hasAttribute("value") ) {
                    this.spacer = e.getAttribute("value");
                }
            }
        }
        catch(Exception e){
            throw new Error("Error reading Schema File: " + this.schema_url);
        }
    }
    
    public Data getData(int index){
        if( index < 0 || index > this.data.length ){
            return null;
        }
        else {
            return this.data[index];
        }
    }
    
    public String[] getDataTypes(){
        String[] dtypes = new String[this.data.length];
        
        for(int i=0; i<this.data.length; i++){
            dtypes[i] = this.data[i].getName();
        }
        
        return dtypes;
    }
    
    public Data getData(String name){
        int index = getDataTypeIndex(name);
        
        if(index == -1 ){
            return null;
        }
        
        return this.data[index];
    }
    
    public int getDataTypeIndex(String name){
        for( int i=0; i<this.data.length; i++){
            if( this.data[i].getName().equals(name) ){
                return i;
            }
        }
        
        return -1;
    }
    
    public int DataTypeCount(){
        return this.data.length;
    }
    
    public boolean hasData(String name){
        for( Data d : data ){
            if( d.getName().equals(name) ){
                return true;
            }
        }
        
        return false;
    }
    
    public String getSpacer(){
        return this.spacer;
    }
    
    public static void main(String[] args){
        Schema schema = new Schema("c:/mydb/master.schema");
        
        
        // list available data
        System.out.println("Available Data Types: ");
        String[] dt = schema.getDataTypes();
        for(int i=0; i<dt.length; i++){
            System.out.println("\t" + dt[i]);
            
            Data d = schema.getData(i);
            System.out.println("\t\tLENGTH: " + d.getLength());
            System.out.println("\t\tMAX. LENGTH: " + d.getMaxlength());
        }
        
        System.out.println("Spacer char: " + schema.getSpacer());
    }
}
