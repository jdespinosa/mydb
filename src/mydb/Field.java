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
public class Field {
    private String name;
    private String datatype;
    private int length;
    
    Field( String name, String dtype, int length ){
        this.name = name;
        this.datatype = dtype;
        this.length = length;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }
    
    public int getLength(){
        return this.length;
    }
    
    
}
