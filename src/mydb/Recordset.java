/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mydb;

/**
 *
 * @author JDE
 * @param <T>
 */
public class Recordset <T> {
    private final Table table;
    private final String recordset_data;      // 
    private final Field[] consideredFields;   // named as such because in a recordset, 
    // .. NOT ALL Tables fields can be considered/included
    private int row;
    private final int record_width;
    private final Schema schema;
    private  int num_rows;
    
    private boolean isEmpty;
    
    public Recordset(Schema schema, Table table, String raw_recordset_data, Field[] fields){
        this.table  = table;
        this.recordset_data = raw_recordset_data;
        this.consideredFields = fields;
        this.row = 0;
        this.record_width = getRecordWidth();
        this.schema = schema;
        this.num_rows = raw_recordset_data.length() / getRecordWidth(); // determine max rows before hand
        
        this.isEmpty = this.num_rows <= 0 || raw_recordset_data.trim().equals("");
        
        if(this.isEmpty){
            this.num_rows = 0;
        }
        
        System.out.println("RAW: " + this.isEmpty);
    }
    
    public boolean isEmpty(){
        return this.isEmpty;
    }
    
    public int getRecordCount(){
        return this.num_rows;
    }
    
    public Record nextRecord(){
        DataValue[] dv = new DataValue[consideredFields.length];
        
        for(int i=0; i<consideredFields.length; i++){
            dv[i] = new DataValue(consideredFields[i].getName(), getFieldValue(consideredFields[i].getName()));
        }
        
        this.row++;
        
        return new Record(dv);
    }
    
    public boolean hasNext(){
        return row < num_rows;
    }
    
    private T getFieldValue(String fieldname ){
        Field field = table.getField(fieldname);
        
        // determine row offset position
        int rowOffset = this.record_width * row;
        
        // determine number of chars to read
        rowOffset += getFieldOffset(fieldname);
        
        String svalue = strim(this.recordset_data.substring(rowOffset, rowOffset+table.getFieldWidth(field)));
        
        // cast it to proper data type based on field type
        switch(field.getDatatype()){
            case "INT":
                int ival = Integer.parseInt(svalue);
                return (T) svalue;
                
                // NOTE: doing 'return (T) Integer.parseInt(svalue);' causes an error that's why the extra step above
                
            case "DOUBLE":
                double dval = Double.parseDouble(svalue);
                return (T) svalue;
                
            case "VARCHAR":
                return (T) svalue;
            
        }
        
        return null;   // theoritically, this can never happen because the switch() return will occur 
    }
    
    private int getFieldOffset(String fieldName){
        String[] flds = table.getFieldNames();
        int fOffsetPos = 0;
        
        for(String fld : flds ){
            Field field = table.getField(fld);
            if( fld.equals(fieldName) ){
                return fOffsetPos;
            }
            
            // System.out.println("field: " + field.getName() + " type: " + schema.getData(field.getDatatype()).getLength());
            int len = schema.getData(field.getDatatype()).getLength();
            if( len <= 0 ){
                len = field.getLength();
            }
            
            // System.out.println("len = " + len);
            
            fOffsetPos += len;
        }
        
        return fOffsetPos;
    }
    
    private int getRecordWidth(){
        int width = 0;
        
        for(Field field : this.consideredFields){
            // int len = table.getFieldWidth(field);
            width += table.getFieldWidth(field);
        }
        return width;
    }
    
    private String strim( String value){
        return value.replace(this.schema.getSpacer(), "");
    }
    
    public static void main(String[] args){
        
    }
    
}
