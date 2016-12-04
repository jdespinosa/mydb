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
public class FilterExpression {
    protected FilterTerm term1;
    protected FilterTerm term2;
    protected DBRelationalOptr rop;
    protected DBLogicalOptr lop;
    protected FilterExpression expr2;

    public FilterExpression( FilterTerm t1, DBRelationalOptr rop, FilterTerm t2,  DBLogicalOptr lop, FilterExpression expr2 ) {
        this.term1 = t1;
        this.term2 = t2;
        this.rop = rop;
        this.lop = lop;
        this.expr2 = expr2;
    }
    
    
    
    
}
