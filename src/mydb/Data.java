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
public class Data {
    private String name;
    private int length;
    private int maxlength;
    
    Data(String name, int length, int maxlength) {
        this.name = name;
        this.length = length;
        this.maxlength = maxlength;
    }

    public String getName() {
        return name;
    }

    
    public int getLength() {
        return length;
    }

    
    public int getMaxlength() {
        return maxlength;
    }

    
    
}
