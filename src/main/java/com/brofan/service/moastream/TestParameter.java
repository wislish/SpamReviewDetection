package com.brofan.service.moastream;

/**
 * Created by wislish on 9/4/15.
 */
public class TestParameter {

    public static void main(String[] args) throws Exception {
        SGD sgd = new SGD();

        sgd.m_weights = new double[6];
        sgd.m_weights[0]=8.817729278679913;
        sgd.m_weights[1]=0.11455106338401641;
        sgd.m_weights[2]=2.136349734039031;
        sgd.m_weights[3]=-0.4214549791317099;
        sgd.m_weights[4]=0;
        sgd.m_weights[5]=-1.571394207740338;

        new TestIncrementalSGD().testModel(sgd);



    }
}
