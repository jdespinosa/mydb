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
public class DataValue<T> {
    private T value;
    private String field;
    
    DataValue(String field, T val){
        this.field = field;
        this.value = val;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }
    
    
    
}
