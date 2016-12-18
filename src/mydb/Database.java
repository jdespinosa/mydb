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
import java.nio.*;
import java.nio.channels.FileChannel;

public class Database<T> {
    private Schema schema;
    private String dburl;
    private String schemaurl;
    private String name;
    
    Database( String dburl, String schemaurl ){
        this.schemaurl = schemaurl;
        this.dburl = dburl;
        
        // attempt to access schema file
        schema = new Schema(this.schemaurl);
        
        // attempt to open database
        try{
            File dbdir = new File(dburl);
            
            if( !dbdir.isDirectory()){
                throw new Error("Invalid database directory path: " + dburl);
            }
            
            // get the direcotry name
            this.name = dbdir.getName();
            
            
        }
        catch(Exception e){
            throw new Error("Cannot open database directory: " + dburl);
        }
    }
    
    public String[] listTables(){
        String[] tbls;
        
        try{
            File dbdir = new File(this.dburl);
            FilenameFilter filter = new FilenameFilter() {
                @Override
                public boolean accept(File file, String string) {
                    return string.toLowerCase().endsWith(".tbl");
                }
            };
                    
            File[] contents = dbdir.listFiles(filter);
            tbls = new String[contents.length];
            for(int i=0; i<contents.length; i++){
                tbls[i] = contents[i].getName().replace(".tbl", "");
            }
            
            return tbls;
        }
        catch(Exception e){
            throw new Error("Error retrieving table listing for [" + this.name + "] database");
        }
    }
    
    public String getName(){
        return this.name;
    }
    
    public Table getTable(int index){
        String[] tbls = listTables(); 
        if( index < 0 || index > tbls.length ){
            return null;
        }
        else {
            System.out.println(tbls[index]);
            Table t = new Table(this.dburl + "/" + tbls[index] + ".tbl", schema);
            return t;
        }
    }
    
    public Table getTable(String name){
        int index = getTableIndex(name);
        
        if(index == -1 ){   // tests if table name is valid
            return null;
        }
        else {
            Table t = new Table(this.dburl + "/" + name + ".tbl", schema);
            return t;
        }
    }
    
    public int deleteRecords(Table table, FilterExpression fexpr){
        // check if filter expression is valid
        if( !filterExprValid(table, fexpr) ){
            return 1;
        }
        else if( fexpr == null  ){      // effect will empty the contents of table
            return 2;                   // do not permit using this method instead, use truncateTable()
        }
        
        int twidth = table.getTableWidth();
        
        try{
           File file = new File(this.dburl + "/" + table.getName() + ".dat");
           RandomAccessFile aFile = new RandomAccessFile(file, "r");
           FileChannel fc = aFile.getChannel();
           
           // temp file
           File tmpFile = File.createTempFile(table.getName(), ".tlmp", file.getParentFile());
           FileWriter fw = new FileWriter(tmpFile, true);
           
           // loop thru every record
           ByteBuffer copy = ByteBuffer.allocate(twidth);
           int nread;
           int row = 0;
           int delCount = 0;
           String record;
           
           do{
               // empty buffer
               copy.rewind();
               
               // read one row from the data
               do{
                   nread = fc.read(copy);
               }while( nread != -1 && copy.hasRemaining() );
               
               record = new String(copy.array());
               
               if( !filterRecord(table, record, fexpr) == true ){
                   System.out.println("HERE");
                    // write to temp. file
                   fw.write(record);
                   
                   // update deletion counter
                   delCount++;
               }
               
               row++;                       // update row indicator
               fc.position(twidth * row);   // use it to update fc position
               
           }while( fc.position() <= fc.size()-1 );
           
           // close file channel and randomfileaccess 
           
           fc.close();
           aFile.close();
           
           fw.flush();
           fw.close();
           
           

            // delete original file
           boolean delres = file.delete();
           
           //System.out.println("Delete file Result: " + delres);
           
           // rename temp to table file
           System.out.println("Rename: " + tmpFile.renameTo(file) );
        }
        catch(Exception e){
            return 3;       
        }
        
        return 0;
    }
    
     public int deleteRecords(String tablename, FilterExpression fexpr){
         Table table = new Table(this.dburl + "/" + tablename + ".tbl", this.schema);
         
        // check if filter expression is valid
        if( !filterExprValid(table, fexpr) ){
            return 1;
        }
        else if( fexpr == null  ){      // effect will empty the contents of table
            return 2;                   // do not permit using this method instead, use truncateTable()
        }
        
        int twidth = table.getTableWidth();
        
        try{
           File file = new File(this.dburl + "/" + table.getName() + ".dat");
           RandomAccessFile aFile = new RandomAccessFile(file, "r");
           FileChannel fc = aFile.getChannel();
           
           // temp file
           File tmpFile = File.createTempFile(table.getName(), ".tlmp", file.getParentFile());
           FileWriter fw = new FileWriter(tmpFile, true);
           
           // loop thru every record
           ByteBuffer copy = ByteBuffer.allocate(twidth);
           int nread;
           int row = 0;
           int delCount = 0;
           String record;
           
           do{
               // empty buffer
               copy.rewind();
               
               // read one row from the data
               do{
                   nread = fc.read(copy);
               }while( nread != -1 && copy.hasRemaining() );
               
               record = new String(copy.array());
               
               if( !filterRecord(table, record, fexpr) == true ){
                   System.out.println("HERE");
                    // write to temp. file
                   fw.write(record);
                   
                   // update deletion counter
                   delCount++;
               }
               
               row++;                       // update row indicator
               fc.position(twidth * row);   // use it to update fc position
               
           }while( fc.position() <= fc.size()-1 );
           
           // close file channel and randomfileaccess 
           fc.close();
           aFile.close();
           
           fw.flush();
           fw.close();
           
           // delete original file
           boolean delres = file.delete();
           
           System.out.println("Delete file Result: " + delres);
           
           // rename temp to table file
           System.out.println("Rename: " + tmpFile.renameTo(file) );
        }
        catch(Exception e){
            return 3;       
        }
        
        return 0;
    }
    
    public int getTableIndex(String name){
        String[] tbls = listTables();
        
        for( int i=0; i<tbls.length; i++){
            if( tbls[i].equals(name) ){
                return i;
            }
        }
        
        return -1;
        
    }
    
    public int createTable(String name, Field[] fields){
        String output="<table name=\"" + name +  "\" >";
        
        // check if table name is valid
        if( !isValidTableName(name)  ){
            return 1;
        }
        
        for(int i=0; i<fields.length; i++){
            // check if name is valid
            if( !isValidFieldName(fields[i].getName())){
                return 2;
            }
            
            if( !isValidFieldType(fields[i].getDatatype()) ){
                return 3;
            }
        
            
            output += "<field name=\"" + fields[i].getName() + "\" type=\"" + fields[i].getDatatype() + "\"";
            
            if( fields[i].getLength() >= 1 ){
                output += " length=\"" + fields[i].getLength()  + "\"";
            }
            
            output += "/>";
        }
        
        output += "</table>";
        
        // write this to file
        try{
            File file = new File(this.dburl + "/" + name + ".tbl");
            FileWriter fw = new FileWriter(file, false);

            fw.write(output, 0, output.length());
            fw.flush();
            fw.close();
        }
        catch(Exception e){
            System.out.println(e.getMessage());
            return 4;
        }
        
        return 0;
    }
    
    public boolean isValidTableName(String name){
        // check if name is not empty
        if(name.length() == 0 ){
            return false;
        }
        
        // is table unique
        if( new File(this.dburl + "/" + name + ".tbl").exists()){
            return false;
        }
        
        return true;
    }
    
    public boolean isValidFieldType(String type){
        return schema.getDataTypeIndex(type) != -1;
    }
    
    public boolean isValidFieldName(String name){
        if( name.length() == 0 ) {
            return false;
        }
        
        return true;
    }
    
    // T here could be INT, STRING or DOUBLE
    public int insertToTable(String name,DataValue[] data ){
        if( !tableExist(name) ){
            return 1;
        }
        
        try{
            File file = new File(this.dburl + "/" + name + ".dat");
            FileWriter fw = new FileWriter(file, true);
            
            // determine correct order
            Table t = new Table(this.dburl + "/" + name + ".tbl", schema);
            int twidth = t.getTableWidth();
            
            String rowdata = "";
            
            String[] flds = t.getFieldNames();
            for( String fld : flds  ){
                DataValue dv = getDataValue(fld, data);
                if( dv == null  ){
                    return 5;
                }
                
                rowdata += padValue(dv.getValue().toString(), t.getFieldWidth(t.getField(fld)), schema.getSpacer());    // converts double and int to string
            }    

            System.out.println(rowdata);
            
            
            fw.write(rowdata, 0, rowdata.length());
            fw.flush();
            fw.close();
        }
        catch(Exception e){
            System.out.println(e.getMessage());
            return 4;
        }
        
        return 0;
    }
    
    public int updateTable(
            String name,            // table name 
            DataValue[] dv,         // field list with their corresponding values
            FilterExpression fexpr  // represent WHERE SQL clause
    ){
        // check if table exists
        if( !tableExist(name) ){
            return 1;
        }
        
         // get Table width
        Table t = new Table(this.dburl + "/" + name + ".tbl", schema);
        
        // Check if fieldList is not null
        if( dv == null ){
            return 2;
        }

        // check if field list is valid
        if( !fieldListValid(t, dv) ){
            return 3;
        }
        
        // check if FilterExpression is valid
        if( !filterExprValid(t, fexpr) ){
            System.out.println("Invalid FilterExpression");
            return 4;
        }
        
        int twidth = t.getTableWidth();

        // open the table
        try{
            File file = new File(this.dburl + "/" + name + ".dat");
            RandomAccessFile aFile = new RandomAccessFile(file, "rw");
            FileChannel fc = aFile.getChannel();
            
            // loop thru every record
            ByteBuffer copy = ByteBuffer.allocate(twidth);
            int nread;
            int row = 0;
            String record;
            
            do{
                // IMPORTANT: empty buffer
                copy.rewind();                              
                        
                // Read one row from the data
                do{
                    nread = fc.read(copy);                    
                }while(nread != -1 && copy.hasRemaining());
                
                record = new String(copy.array());
                
                if( filterRecord(t, record, fexpr) == true ){
                    System.out.println(record + ": " + filterRecord(t, record, fexpr) );
                    updateRecord(t, dv, fc, row);
                }
                
                row++;                          // update row
                fc.position(twidth*row);        // point to next data
            }while( fc.position() <= fc.size()-1  );
            
        }
        catch(Exception e){
            
        }
        return 0;
    }
    
    public int updateRecord(Table table, DataValue[] dv, FileChannel fc, int row){
        int twidth = table.getTableWidth();
        int rowOffset = twidth * row;   // point to correct "row" in the file
        int initFilePos;
        
        try{
            for(int i=0; i<dv.length; i++){
                
                initFilePos = rowOffset;
                
                String fname = dv[i].getField();
                System.out.println(fname);
                Field field = table.getField(fname);
                        
                // calculate starting field position in proper row
                initFilePos += getFieldOffset(table, fname);
                
                System.out.println("*** " + fname +  " ifp=" + initFilePos);

                // get fieldWidth
                int fwidth = getFieldWidth(table, fname);
                
                // Go to position of field
                fc.position(initFilePos);
                
                // change field value
                String fieldValue = padValue(dv[i].getValue().toString(), fwidth, schema.getSpacer());
                byte[] data = fieldValue.getBytes();
                ByteBuffer out = ByteBuffer.wrap(data);
                while(out.hasRemaining()){
                    fc.write(out);
                }
                
                
            }
        }
        catch(Exception ex){
            System.out.println("Error occured while updating record. " + ex.toString());
            return 1;
        }
        
        return 0;
    }
    
    protected int getFieldWidth(Table table, String fname){
        Field field = table.getField(fname);
                
        int fwidth = schema.getData(field.getDatatype()).getLength();
        if(fwidth <= 0 ){
            fwidth = field.getLength();
        }
        
        return fwidth;
    }
    
    public int getFieldOffset(Table table, String fieldName){
        String[] flds = table.getFieldNames();
        int fOffsetPos = 0;
        
        for(String fld : flds ){
            Field field = table.getField(fld);
            if( fld.equals(fieldName) ){
                return fOffsetPos;
                // break;
            }
            
            System.out.println("field: " + field.getName() + " type: " + schema.getData(field.getDatatype()).getLength());
            int len = schema.getData(field.getDatatype()).getLength();
            if( len <= 0 ){
                len = field.getLength();
            }
            
            System.out.println("len = " + len);
            
            fOffsetPos += len;
        }
        
        return fOffsetPos;
    }
    
    
    public boolean fieldListValid(Table table, DataValue[] dv){
        // Table table = new Table(this.dburl + "/" + tableName + ".tbl", this.schema);
        for(DataValue d : dv){
            if( table.getField(d.getField()) == null  ){
                return false;
            }
            else if(fieldValueValid(table, d) == false){
                return false;
            }
        }
        
        return true;
    }
    
    public boolean filterExprValid(Table table, FilterExpression fexpr){
        /*
            Filter Expression Terms states:
            state 1: Term1 is Field,        Term2 is Constant
            state 2: Term1 is Field,        Term2 is Field
            state 3: Term1 is Constant,     Term2 is Field
            state 4: Term1 is Constant,     Term2 is Constant
        */
        String term1_tag, term2_tag;
        
        if( fexpr == null ){    // it means no filter
            return true;
        }

        // check first if term 1 is a valid field
        if( fexpr.term1.term instanceof mydb.Field ){
            String field_name = (String) fexpr.term1.getValue();
            
            // check if field name is found in the table
            if( table.getField(field_name) == null  ){
                System.out.println("Error: Field " + field_name + " not found in Table " + table.getName());
                return false;
            }
            
            // System.out.println("Term1 is a Field: " + fexpr.term1.getValue());
            
            term1_tag = "Field";
        }
        else {
            term1_tag = "Constant";
            // System.out.println("Term1 is a Constant: " + fexpr.term1.getValue());
        }
        
        if( fexpr.term2.term instanceof mydb.Field ){
            String field_name = (String) fexpr.term2.getValue();
            
            // check if field name is found in the table
            if( table.getField(field_name) == null  ){
                System.out.println("Error: Field " + field_name + " not found in Table " + table.getName());
                return false;
            }
            
            term2_tag = "Field";
            // System.out.println("Term2 is a Field: " + fexpr.term2.getValue() );
        }
        else {
            term2_tag = "Constant";
            // System.out.println("Term2 is a Constant: " + fexpr.term2.getValue());
        }
        
        String state = term1_tag + "," + term2_tag;
        
        // check if term combinations are valid
        switch(state){
            case "Field,Constant":
                // check value of constant if matches field data type
                if( fexpr.term1.getType().equals("INT") && !fexpr.term2.getType().equals("java.lang.Integer") ){
                    System.out.println("Error: value of Field " + fexpr.term1.getValue() + " is invalid: " + fexpr.term2.getType());
                    return false;
                }
                else if( fexpr.term1.getType().equals("DOUBLE") && !fexpr.term2.getType().equals("java.lang.Double") ){
                    // System.out.println("FC DOUBLE");
                    System.out.println("Error: value of Field " + fexpr.term1.getValue() + " is invalid: " + fexpr.term2.getType());
                    return false;
                }
                else if( fexpr.term1.getType().equals("VARCHAR") && !fexpr.term2.getType().equals("java.lang.String") ){
                    // System.out.println("FC VARCHAR");
                    System.out.println("Error: value of Field " + fexpr.term1.getValue() + " is invalid: " + fexpr.term2.getType());
                    return false;
                }
                break;
            case "Field,Field":
            case "Constant,Constant":
                if( !fexpr.term1.getType().equals(fexpr.term2.getType()) ){
                    System.out.println("Error: Constant" + fexpr.term1.getValue() + " doesn't match type of Constant " + fexpr.term2.getValue() );
                    return false;
                }
                break;
            case "Constant,Field":
                // check value of constant if matches field data type
                if( fexpr.term2.getType().equals("INT") && !fexpr.term1.getType().equals("java.lang.Integer") ){
                    System.out.println("Error: value of Field " + fexpr.term2.getValue() + " is invalid: " + fexpr.term1.getType());
                    return false;
                }
                else if( fexpr.term2.getType().equals("DOUBLE") && !fexpr.term1.getType().equals("java.lang.Double") ){
                    // System.out.println("CF DOUBLE");
                    System.out.println("Error: value of Field " + fexpr.term2.getValue() + " is invalid: " + fexpr.term1.getType());
                    return false;
                }
                else if( fexpr.term2.getType().equals("VARCHAR") && !fexpr.term1.getType().equals("java.lang.String") ){
                    // System.out.println("CF VARCHAR");
                    System.out.println("Error: value of Field " + fexpr.term2.getValue() + " is invalid: " + fexpr.term1.getType());
                    return false;
                }
                break;
        }
        
        // check if Relational Operator is valid
        if( fexpr.rop == null  ){
            // System.out.println("ROP NULL");
            System.out.println("Error: Relational Operator is invalid or null");
            return false;
        }
        
        // check if Logical Operator is valid
        //if( fexpr.lop != null ){
        //    return false;
       // }
        
        // check if it has some inner Filter Expressions
        if( fexpr.lop != null && fexpr.expr2 != null  ){
             return true && filterExprValid(table, fexpr.expr2); 
        }
        
        return true;
    }
    
    /* PRIVATE Because this can only be called AFTER FilterExpression has been validated */
    private <T> boolean filterRecord(Table table, String record_data, FilterExpression fexpr){
        String term1_tag, term2_tag;
        
        if(fexpr == null ){ // this means there is no filter
            return true;    
        }
        
        // check first if term 1 is a valid field
        if( fexpr.term1.term instanceof mydb.Field ){
            String field_name = (String) fexpr.term1.getValue();
            
            // check if field name is found in the table
            if( table.getField(field_name) == null  ){
                return false;
            }
            
            // System.out.println("Term1 is a Field: " + fexpr.term1.getValue());
            
            term1_tag = "Field";
        }
        else {
            term1_tag = "Constant";
            // System.out.println("Term1 is a Constant: " + fexpr.term1.getValue());
        }
        
        if( fexpr.term2.term instanceof mydb.Field ){
            String field_name = (String) fexpr.term2.getValue();
            
            // check if field name is found in the table
            if( table.getField(field_name) == null  ){
                return false;
            }
            
            term2_tag = "Field";
            //System.out.println("Term2 is a Field: " + fexpr.term2.getValue() );
        }
        else {
            term2_tag = "Constant";
            //System.out.println("Term2 is a Constant: " + fexpr.term2.getValue());
        }
        
        String state = term1_tag + "," + term2_tag;
        
        // check if term combinations are valid
        switch(state){
            case "Field,Constant":
                // reference the Field
                String raw_field_data = getFieldValue(table, record_data, (String)fexpr.term1.getValue());
                
                switch(fexpr.term1.getType()){
                    case "INT":
                        //System.out.println("INT");
                        if( fexpr.expr2 != null ){
                            switch(fexpr.lop){
                                case AND:
                                    return performRelOprn(Integer.parseInt(raw_field_data), (Integer)fexpr.term2.getValue(), fexpr.rop, "java.lang.Integer") && 
                                    filterRecord(table, record_data, fexpr.expr2);
                                    
                                case OR:
                                    return performRelOprn(Integer.parseInt(raw_field_data), (Integer)fexpr.term2.getValue(), fexpr.rop, "java.lang.Integer") ||
                                    filterRecord(table, record_data, fexpr.expr2);
                            }
                        }
                        return performRelOprn(Integer.parseInt(raw_field_data), (Integer)fexpr.term2.getValue(), fexpr.rop, "java.lang.Integer");
                        // break;
                    case "DOUBLE":
                        if( fexpr.expr2 != null ){
                            switch(fexpr.lop){
                                case AND:
                                    return performRelOprn(Double.parseDouble(raw_field_data), (Double)fexpr.term2.getValue(), fexpr.rop, "java.lang.Double") &&
                                    filterRecord(table, record_data, fexpr.expr2);
                                    
                                case OR:
                                    return performRelOprn(Double.parseDouble(raw_field_data), (Double)fexpr.term2.getValue(), fexpr.rop, "java.lang.Double") ||
                                    filterRecord(table, record_data, fexpr.expr2);
                            }
                        }
                        else{
                            return performRelOprn(Double.parseDouble(raw_field_data), (Double)fexpr.term2.getValue(), fexpr.rop, "java.lang.Double");
                        }
   
                    case "VARCHAR":
                        // System.out.println("Term1 = " + raw_field_data + ", Term2 = " + fexpr.term2.getValue().toString()  );
                        if( fexpr.expr2 != null){
                            switch(fexpr.lop){
                                case AND:
                                    return performRelOprn(raw_field_data, fexpr.term2.getValue().toString(), fexpr.rop, "java.lang.String") &&
                                    filterRecord(table, record_data, fexpr.expr2);
                                            
                                case OR:
                                    return performRelOprn(raw_field_data, fexpr.term2.getValue().toString(), fexpr.rop, "java.lang.String") ||
                                    filterRecord(table, record_data, fexpr.expr2);                                 
                            } 
                        }
                        else {
                            return performRelOprn(raw_field_data, fexpr.term2.getValue().toString(), fexpr.rop, "java.lang.String");
                        }
                }
                
                break;
            case "Field,Field":
                String raw_field_data1 = getFieldValue(table, record_data, (String)fexpr.term1.getValue());
                String raw_field_data2 = getFieldValue(table, record_data, (String)fexpr.term2.getValue());
                
                switch(fexpr.term1.getType()){
                    case "INT":
                        if(fexpr.expr2 != null ){
                            switch(fexpr.lop){
                                case AND:
                                    return performRelOprn(Integer.parseInt(raw_field_data1), Integer.parseInt(raw_field_data2), fexpr.rop, "java.lang.Integer") &&
                                            filterRecord(table, record_data, fexpr.expr2);
                                case OR:
                                    return performRelOprn(Integer.parseInt(raw_field_data1), Integer.parseInt(raw_field_data2), fexpr.rop, "java.lang.Integer") ||
                                            filterRecord(table, record_data, fexpr.expr2);
                            }
                        }
                        else{
                            return performRelOprn(Integer.parseInt(raw_field_data1), Integer.parseInt(raw_field_data2), fexpr.rop, "java.lang.Integer");
                        }
                        // break;
                    case "DOUBLE":
                        if(fexpr.expr2 != null){
                            switch(fexpr.lop){
                                case AND:
                                   return performRelOprn(Double.parseDouble(raw_field_data1), Double.parseDouble(raw_field_data2), fexpr.rop, "java.lang.Double") &&
                                         filterRecord(table, record_data, fexpr.expr2);
                                case OR:
                                    return performRelOprn(Double.parseDouble(raw_field_data1), Double.parseDouble(raw_field_data2), fexpr.rop, "java.lang.Double") ||
                                         filterRecord(table, record_data, fexpr.expr2);
                            }
                        }
                        else {
                            return performRelOprn(Double.parseDouble(raw_field_data1), Double.parseDouble(raw_field_data2), fexpr.rop, "java.lang.Double");
                        }
                    case "VARCHAR":
                        if( fexpr.expr2 != null ){
                            switch(fexpr.lop){
                                case AND:
                                    return performRelOprn(raw_field_data1, raw_field_data1, fexpr.rop, "java.lang.String") &&
                                            filterRecord(table, record_data, fexpr.expr2);
                                case OR:
                                    return performRelOprn(raw_field_data1, raw_field_data1, fexpr.rop, "java.lang.String") ||
                                            filterRecord(table, record_data, fexpr.expr2);
                            }
                        }
                        else {
                            return performRelOprn(raw_field_data1, raw_field_data1, fexpr.rop, "java.lang.String");
                        }
                }
                
                break;
                
            case "Constant,Constant":
                switch(fexpr.term1.getType()){
                    case "java.lang.Integer":
                        if( fexpr.expr2 != null ){
                            switch(fexpr.lop){
                                case AND:
                                    return performRelOprn((Integer)fexpr.term1.getValue(), (Integer)fexpr.term2.getValue(), fexpr.rop, "java.lang.Integer") &&
                                            filterRecord(table, record_data, fexpr.expr2);
                                case OR:
                                    return performRelOprn((Integer)fexpr.term1.getValue(), (Integer)fexpr.term2.getValue(), fexpr.rop, "java.lang.Integer") ||
                                            filterRecord(table, record_data, fexpr.expr2);
                            }
                        }
                        else {
                            return performRelOprn((Integer)fexpr.term1.getValue(), (Integer)fexpr.term2.getValue(), fexpr.rop, "java.lang.Integer");
                        }
                        // break;
                    case "java.lang.Double":
                        if(fexpr.expr2 != null){
                            switch(fexpr.lop){
                                case AND:
                                    return performRelOprn((Double)fexpr.term1.getValue(), (Double)fexpr.term2.getValue(), fexpr.rop, "java.lang.Double") &&
                                            filterRecord(table, record_data, fexpr.expr2);
                                case OR:
                                    return performRelOprn((Double)fexpr.term1.getValue(), (Double)fexpr.term2.getValue(), fexpr.rop, "java.lang.Double") ||
                                            filterRecord(table, record_data, fexpr.expr2);
                            }
                        }
                        else {
                            return performRelOprn((Double)fexpr.term1.getValue(), (Double)fexpr.term2.getValue(), fexpr.rop, "java.lang.Double");
                        }
                        
                    case "java.lang.String":
                        if( fexpr.expr2 != null ){
                            switch(fexpr.lop){
                                case AND:
                                    return performRelOprn(fexpr.term1.getValue().toString(), fexpr.term2.getValue().toString(), fexpr.rop, "java.lang.String") &&
                                            filterRecord(table, record_data, fexpr.expr2);
                                case OR:
                                    return performRelOprn(fexpr.term1.getValue().toString(), fexpr.term2.getValue().toString(), fexpr.rop, "java.lang.String") ||
                                            filterRecord(table, record_data, fexpr.expr2);
                            }
                        }
                        else {
                            return performRelOprn(fexpr.term1.getValue().toString(), fexpr.term2.getValue().toString(), fexpr.rop, "java.lang.String");
                        }
                }
                
                break;
            case "Constant,Field":
                String raw_field_data3 = getFieldValue(table, record_data, (String)fexpr.term2.getValue());
                switch(fexpr.term2.getType()){
                    case "INT":
                        if(fexpr.expr2 != null){
                           switch(fexpr.lop){
                               case AND:
                                   return performRelOprn( (Integer)fexpr.term1.getValue(),  Integer.parseInt(raw_field_data3), fexpr.rop, "java.lang.Integer") &&
                                           filterRecord(table, record_data, fexpr.expr2);
                               case OR:
                                   return performRelOprn( (Integer)fexpr.term1.getValue(),  Integer.parseInt(raw_field_data3), fexpr.rop, "java.lang.Integer") ||
                                           filterRecord(table, record_data, fexpr.expr2);
                           }
                        }
                        else {
                            return performRelOprn( (Integer)fexpr.term1.getValue(),  Integer.parseInt(raw_field_data3), fexpr.rop, "java.lang.Integer");
                        }
                        // break;
                    case "DOUBLE":
                        if(fexpr.expr2 != null){
                            switch(fexpr.lop){
                               case AND:
                                   return performRelOprn((Double)fexpr.term1.getValue(),  Double.parseDouble(raw_field_data3), fexpr.rop, "java.lang.Double") &&
                                           filterRecord(table, record_data, fexpr.expr2);
                               case OR:
                                   return performRelOprn((Double)fexpr.term1.getValue(),  Double.parseDouble(raw_field_data3), fexpr.rop, "java.lang.Double") ||
                                           filterRecord(table, record_data, fexpr.expr2);
                           }
                        }
                        else {
                            return performRelOprn((Double)fexpr.term1.getValue(),  Double.parseDouble(raw_field_data3), fexpr.rop, "java.lang.Double");
                        }
                        
                    case "VARCHAR":
                        if(fexpr.expr2 != null){
                            switch(fexpr.lop){
                               case AND:
                                   return performRelOprn(fexpr.term1.getValue().toString(),  raw_field_data3, fexpr.rop, "java.lang.String") &&
                                           filterRecord(table, record_data, fexpr.expr2);
                               case OR:
                                   return performRelOprn(fexpr.term1.getValue().toString(),  raw_field_data3, fexpr.rop, "java.lang.String") ||
                                           filterRecord(table, record_data, fexpr.expr2);
                            }
                        }
                        else {
                            return performRelOprn(fexpr.term1.getValue().toString(),  raw_field_data3, fexpr.rop, "java.lang.String");
                        }
                }
                
                break;
        }
        
        return true;
    }
    
    public <T> boolean performRelOprn(T term1, T term2, DBRelationalOptr rol, String type){
        System.out.print(term1);
        if( rol != DBRelationalOptr.LIKE ){
           if( null != rol )switch (rol) {
                case EQUAL:
                    System.out.println(" == " + term2);
                    if( type.equals("java.lang.String") ){
                        return term1.equals(term2);
                    }
                    else {
                        return term1 == term2;
                    }
                    
                case GREATER_THAN:
                    System.out.println(" > " + term2);
                    if(type.equals("java.lang.Double")){
                        return (Double) term1 > (Double) term2;
                    }
                    else if(type.equals("java.lang.Integer")){
                        return (Integer) term1 > (Integer) term2;
                    }
                    
                    throw new Error("Cannot perform > Operation to Type String");
                case GREATER_THAN_EQUAL:
                    System.out.println(" >= " + term2);
                    if(type.equals("java.lang.Double")){
                        return (Double) term1 >= (Double) term2;
                    }
                    else if(type.equals("java.lang.Integer")){
                        return (Integer) term1 >= (Integer) term2;
                    }
                    
                    throw new Error("Cannot perform >= Operation to Type String");
                case LESS_THAN:
                    System.out.println(" < " + term2);
                    if(type.equals("java.lang.Double")){
                        return (Double) term1 < (Double) term2;
                    }
                    else if(type.equals("java.lang.Integer")){
                        return (Integer) term1 < (Integer) term2;
                    }
                    
                    throw new Error("Cannot perform < Operation to Type String");
                case LESS_THAN_EQUAL:
                    System.out.println(" <= " + term2);
                    if(type.equals("java.lang.Double")){
                        return (Double) term1 <= (Double) term2;
                    }
                    else if(type.equals("java.lang.Integer")){
                        return (Integer) term1 <= (Integer) term2;
                    }
                    
                    throw new Error("Cannot perform <= Operation to Type String");
                case NOT_EQUAL:
                    System.out.println(" != " + term2);
                    if( type.equals("java.lang.String") ){
                        return !term1.equals(term2);
                    }
                    else {
                        return !(term1 == term2);
                    }
                default:
                    break;
            }
            
        }
        else {  // For LIKE operations for String only data
            return isLIKE((String)term1, (String)term2);
        }
        
        return false;
    }
    
    public boolean isLIKE(String term1, String term2){
        
        return true;
    }
    
    
    public String getFieldValue(Table table, String record, String fieldName ){
        // determine position of field in row
        int fOffsetPos = 0;
        int fwidth=0;
        String[] flds = table.getFieldNames();
        for( String fld : flds ){
            Field field = table.getField(fld);
            
            if( fld.equals(fieldName) ){
                fwidth = schema.getData(field.getDatatype()).getLength();
                
                if( fwidth <= 0 ){  // for VARCHAR Field
                    fwidth = field.getLength();
                }
                
                break;
            }
            
            int len = schema.getData(field.getDatatype()).getLength();
            if( len <= 0 ){ // means a VARCHAR
                len = field.getLength();    // means no. of chars
            }
            
            fOffsetPos += len;
        }
        
        
        String raw_data = record.substring(fOffsetPos, fOffsetPos+fwidth);
        String polished_data = raw_data.replaceAll(schema.getSpacer(), "");
        
        // System.out.println("RAW: " + raw_data);
        // System.out.println("RAW: " + polished_data);
        
        return polished_data;
    }
    
    
    
    public boolean fieldValueValid(Table table, DataValue dv ){
        Field field =  table.getField(dv.getField());
        
        if( field.getDatatype().equals("VARCHAR") && dv.getValue().getClass().getName().equals("java.lang.String") ){
            //System.out.println("STRING Sya");
            return true;
        }
        else if( field.getDatatype().equals("INT") && dv.getValue().getClass().getName().equals("java.lang.Integer") ){
            //System.out.println("INT Sya");
            return true;
        }
        else if( field.getDatatype().equals("DOUBLE") && dv.getValue().getClass().getName().equals("java.lang.Double") ){
            //System.out.println("DOUBLE Sya");
            return true;
        }
        
        /* {ADD SOMETHING FOR ADDITIONAL TYPES} */
        
        return false;
    }
    
    public boolean performFilterExpr(DataValue[] dv, FilterExpression fexpr){
        
        return true;
    }
    
    public DataValue getDataValue(String name, DataValue[] source){
        for( DataValue dv : source ){
            if( dv.getField().equals(name) ){
                return dv;
            }
        }
        
        return null;
    }
    
    public String padValue( String value, int width, String padding ){
        //System.out.println(value + " " + width);
        
        int diff = width - value.length();
        String buff = "";
        
        for(int i=1; i<=diff; i++){
            buff += padding;
        }
        
        buff += value;
        
        return buff;
    }
    
    public Recordset getRecords(Table table, Field[] fields, FilterExpression fexpr ){
        // check if table exists
        if( !tableExist(table.getName()) ){
            System.out.println("getRecords(): Table " + table.getName() + " doesn't exist.");
            return null;
        }
        
        
        
        // get Table width
        //Table t = new Table(this.dburl + "/" + table.getName() + ".tbl", schema);
        
        // check if FilterExpression is valid
        if( !filterExprValid(table, fexpr) ){
            System.out.println("getRecords(): Invalid FilterExpression");
            return null;
        }
        
        // check if fields have no duplicate
        if( !fieldSetUnique(fields)  ){
            System.out.println("getRecords: Fieldset not unique!");
            return null;
        }
        
        int twidth = table.getTableWidth();
        String raw_table_data = "";
        
        // open the table
        try{
            File file = new File(this.dburl + "/" + table.getName() + ".dat");
            RandomAccessFile aFile = new RandomAccessFile(file, "rw");
            FileChannel fc = aFile.getChannel();
        
            // loop thru every record
            ByteBuffer copy = ByteBuffer.allocate(twidth);
            int nread;
            int row = 0;
            String record;
            
            do{
                // IMPORTANT: empty buffer
                copy.rewind();                              
                
                // Read one row from the data
                do{
                    nread = fc.read(copy);                    
                }while(nread != -1 && copy.hasRemaining());
                
                record = new String(copy.array());
                
                if( filterRecord(table, record, fexpr) == true ){
                    // get field value
                    raw_table_data += extractFieldData(record, fields, table);
                }
                
                row++;                          // update row
                fc.position(twidth*row);        // point to next data
                
            }while( fc.position() <= fc.size()-1);
            
            
            Recordset rs = new Recordset(schema, table, raw_table_data, fields);
            return rs;
            
        }
        catch(Exception e){
            System.out.println("Error encountered: " + e.toString());
        }
        return null;
    }
    
    public String extractFieldData(String record, Field[] fields, Table table){
        String res = "";
        
        int fOffset;
        int fWidth;
        
        for(int i=0; i<fields.length; i++){
            fOffset = getFieldOffset(table, fields[i].getName());
            fWidth = getFieldWidth(table, fields[i].getName());
            
            String value = record.substring(fOffset, fOffset+fWidth);
            
            res+= padValue(value, fWidth, this.schema.getSpacer()); 
        }
        
        return res;
    }   
    
    public T toCorrectData(String val, Field field){
        
        System.out.println("field name: " + field.getName() + " type: " + field.getDatatype());
        
        switch(field.getDatatype()){
            case "INT":
                Integer ival = Integer.parseInt(val);
                System.out.println("1");
                return (T)ival;
                
            case "VARCHAR":
                System.out.println("2");
                return (T)val;
                
            case "DOUBLE":
                System.out.println("3");
                Double dval = Double.parseDouble(val);
                return (T)dval;
                
        }
        System.out.println("4");
        return null;
    }
    
    public boolean fieldSetUnique(Field[] fields){
        for( int i=0; i<fields.length; i++ ){
            for(int j=0; j<fields.length; j++){
                if( j != i && fields[i].getName().equals(fields[j].getName()) ){
                    return false;
                }
            }
        }
        return true;
    }
    
    public boolean tableExist(String name){
        return new File(this.dburl + "/" + name + ".tbl" ).exists();
    }
    
    public static void main(String[] args){
        Database db = new Database("c:/mydb/db1", "c:/mydb/master.schema");
        
        String tname = "student";
        System.out.println("\nContents of table [" + tname + "]" );
        
        DataValue[] dv = new DataValue[2];  // convert this to hashmap
        dv[0] = new DataValue("id", "111-111");
        dv[1] = new DataValue("lname", "Aurillo");
        //dv[2] = new DataValue("fname", "Mark Dominique");
        
        FilterExpression fexpr, fexpr2, fexpr3;
        Table table = new Table(db.dburl + "/" + tname + ".tbl", db.schema);
        Field id = table.getField("id");
        Field lname = table.getField("lname");
        Field age = table.getField("age");
        
        fexpr2 = new FilterExpression(new FilterTerm(lname), DBRelationalOptr.EQUAL, new FilterTerm("Espinosa"),  null, null);
        fexpr3 = new FilterExpression(new FilterTerm(lname), DBRelationalOptr.EQUAL, new FilterTerm("Carugda"), DBLogicalOptr.OR, fexpr2);
        db.updateTable(tname, dv, fexpr3);
        
       
    }
}
