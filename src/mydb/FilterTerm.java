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

public class FilterTerm<T> {
    protected T term;

    public FilterTerm(T term) {
        this.term = term;
    }
    
    public String getType(){
        if( this.term instanceof mydb.Field ){
            Field f = (Field) this.term;
            
            // return the datat type of the field
            return f.getDatatype();
        }
        
        // return data type of constant literal
        return this.term.getClass().getName();
    }
    
    public T getValue(){
        if( this.term instanceof mydb.Field ) {
            Field f = (Field) this.term;
            
            // return the name of the field
            return (T) f.getName();
        }
        
        // return the value of the constant literal 
        return this.term;
    }
    
    public static void main(String[] args){
        FilterTerm ft = new FilterTerm<>(new Field("age", "STRING", 0));
        FilterTerm ft2 = new FilterTerm<>("Hello");
        
        System.out.println(ft.getValue());
        System.out.println(ft.getType());
        
        System.out.println(ft2.getValue());
        System.out.println(ft2.getType());
    }
    
    
    
    
}
