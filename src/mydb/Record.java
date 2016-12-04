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
public class Record<T> {
    private DataValue[] dv;
    private int fieldCount;
    private String[] fieldList;
    
    public Record(DataValue[] dv){
        this.dv = dv.clone();
        this.fieldCount = dv.length;
        
        // get field list
        fieldList = new String[dv.length];
        for(int i=0; i<this.dv.length; i++){
            this.fieldList[i] = this.dv[i].getField();
        }
    }
    
    public T getFieldValue(String fieldname){
        int index = getFieldIndex(fieldname);
        if( index == -1  ){
            return null;
        }
        else {
            return (T) dv[index].getValue();
        }
    }
    
    public int getFieldIndex(String fieldname){
        for(int i=0; i<fieldList.length; i++){
            if( fieldList[i].equals(fieldname) ){
                return i;
            }
        }
        
        return -1;
    }
    
    
}
