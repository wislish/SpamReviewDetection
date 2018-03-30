package com.brofan.service.tester;

/**
 * Created by wislish on 15/9/15.
 */
public class ExecutiveWage extends Wage {

    private int bonus;

    public ExecutiveWage(int salary,int bonus){
        super(salary);
        this.bonus= bonus;
    }
}
