/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

/**
 *
 * @author DavideDP
 */
public class UpdatableNumber 
{
    double number;

    public UpdatableNumber(double number) {
        this.number = number;
    }
    
    public UpdatableNumber() {
        this(0);
    }
    
    public double getNumber() {
        return number;
    }

    public void setNumber(double number) {
        this.number = number;
    }
    
    public void add(double a) {
    	number += a;
    }
    
}
