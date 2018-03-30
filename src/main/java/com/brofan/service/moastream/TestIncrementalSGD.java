package com.brofan.service.moastream;

import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Enumeration;

/**
 * Created by wislish on 9/2/15.
 */
public class TestIncrementalSGD {

    /** The regularization parameter */
    protected double m_lambda = 0.1;

    public void setM_lambda(double m_lambda) {
        this.m_lambda = m_lambda;
    }

    /** The learning rate */
    protected double m_learningRate = 0.001;

    protected static final int HINGE = 0;

    protected static final int LOGLOSS = 1;

    protected static final int SQUAREDLOSS = 2;

    /** The number of training instances */
    protected double m_numInstances;

    protected  Instances instances;

    protected  Instances instances2;

    public static void main(String[] args) throws Exception {

        TestIncrementalSGD a = new TestIncrementalSGD();
        a.run("/Users/wislish/user20.arff");
        a.testParameters();

    }

    public  void run(String path){

        SGD sgd = setSGD();

        try {
            instances = readFile(path);
            instances.setClassIndex(instances.numAttributes()-1);
            Enumeration<Instance> allinstances =instances.enumerateInstances();
            System.out.println(instances.numAttributes());
            while (allinstances.hasMoreElements()){
                Instance arow = allinstances.nextElement();
                sgd.trainOnInstanceImpl(arow);
//                if (sgd.m_t % 10000 ==0){
//                    System.out.println(sgd);
//                }
            }
            System.out.println(sgd);
            testModel(sgd);
        } catch (Exception e) {
            e.printStackTrace();
        }



    }

    public void testModel(SGD sgd) throws IOException{

        Enumeration<Instance> allinstances =instances.enumerateInstances();
        double num = 0.0;
        double num2 =0.0;
        double spamnum=0.0;
//        FileWriter wr = new FileWriter("/home/wislish/Documents/prob1.txt");
        while (allinstances.hasMoreElements()){
            Instance arow = allinstances.nextElement();
            double[] result = sgd.getVotesForInstance(arow);
            if ((result[1] > 0.5 && arow.classValue()==1)||(result[1]<0.5 && arow.classValue()==0)){
                num++;
            }
            if(result[1]> 0.5){
                num2++;
            }
            if(arow.classValue() ==1){
                spamnum++;
            }
//            if (num2 % 4000 ==0){
//                System.out.println(num / instances.numInstances());
//            }
//            num2++;
//            wr.write(result[1] + "\n");


        }
//        wr.close();
        System.out.println("predict spam num:"+num2+"\n real spam num: "+spamnum);
//
        System.out.println(num / instances.numInstances());
    }

    public void testParameters(){
        SGD sgd = new SGD();

        sgd.m_weights = new double[6];
        sgd.m_weights[0]=-8.817729278679913;
        sgd.m_weights[1]=-0.11455106338401641;
        sgd.m_weights[2]=-2.136349734039031;
        sgd.m_weights[3]=0.4214549791317099;
        sgd.m_weights[4]=0;
        sgd.m_weights[5]=1.571394207740338;

//        sgd.m_weights = new double[8];
//        sgd.m_weights[0]=-0.6058956667597551;
//        sgd.m_weights[1]=-4.036079275063497;
//        sgd.m_weights[2]=-2.258874305447077;
//        sgd.m_weights[3]=0.2682255042071049;
//        sgd.m_weights[4]=0.050724650406688096;
//        sgd.m_weights[5]=-0.2550515401637633;
//        sgd.m_weights[6] = 0;
//        sgd.m_weights[7]=13.07917641139016;

        sgd.setLossFunction(LOGLOSS);


        try{
            instances2 = readFile("/Users/wislish/sgd.arff");
            instances2.setClassIndex(instances2.numAttributes() - 1);
            testModel(sgd);
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    public Instances readFile(String path) throws Exception{

        DataSource source = new DataSource(path);
        Instances instances = source.getDataSet();
        m_numInstances = instances.numInstances();
        return instances;
    }

    public SGD setSGD(){

        SGD sgd = new SGD();
        sgd.reset();
        sgd.setLambda(m_lambda);
        sgd.setLearningRate(m_learningRate);
        sgd.setLossFunction(LOGLOSS);
        sgd.m_numInstances = m_numInstances;

        return sgd;
    }

}
