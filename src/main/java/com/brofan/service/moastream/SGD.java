package com.brofan.service.moastream;

/**
 * Created by wislish on 8/28/15.
 */
import moa.classifiers.AbstractClassifier;
import moa.core.Measurement;
import moa.core.StringUtils;
import moa.options.FloatOption;
import moa.options.MultiChoiceOption;
import weka.core.Instance;
import weka.core.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class SGD extends AbstractClassifier {

    /** For serialization */
    private static final long serialVersionUID = -3732968666673530290L;

    @Override
    public String getPurposeString() {
        return "Stochastic gradient descent for learning various linear models (binary class SVM, binary class logistic regression and linear regression).";
    }

    /** The regularization parameter */
    protected double m_lambda = 0.0001;

    public FloatOption lambdaRegularizationOption = new FloatOption("lambdaRegularization",
            'l', "Lambda regularization parameter .",
            0.0001, 0.00, Integer.MAX_VALUE);

    /** The learning rate */
    protected double m_learningRate = 0.01;

    public FloatOption learningRateOption = new FloatOption("learningRate",
            'r', "Learning rate parameter.",
            0.0001, 0.00, Integer.MAX_VALUE);

    /** Stores the weights (+ bias in the last element) */
    protected double[] m_weights;

    /** Holds the current iteration number */
    protected double m_t;

    /** The number of training instances */
    protected double m_numInstances;

    protected static final int HINGE = 0;

    protected static final int LOGLOSS = 1;

    protected static final int SQUAREDLOSS = 2;

    /** The current loss function to minimize */
    protected int m_loss = HINGE;

    public MultiChoiceOption lossFunctionOption = new MultiChoiceOption(
            "lossFunction", 'o', "The loss function to use.", new String[]{
            "HINGE", "LOGLOSS", "SQUAREDLOSS"}, new String[]{
            "Hinge loss (SVM)",
            "Log loss (logistic regression)",
            "Squared loss (regression)"}, 0);

    /**
     * Set the value of lambda to use
     *
     * @param lambda the value of lambda to use
     */
    public void setLambda(double lambda) {
        m_lambda = lambda;
    }

    /**
     * Get the current value of lambda
     *
     * @return the current value of lambda
     */
    public double getLambda() {
        return m_lambda;
    }

    /**
     * Set the loss function to use.
     *
     * @param function the loss function to use.
     */
    public void setLossFunction(int function) {
        m_loss = function;
    }

    /**
     * Get the current loss function.
     *
     * @return the current loss function.
     */
    public int getLossFunction() {
        return m_loss;
    }

    /**
     * Set the learning rate.
     *
     * @param lr the learning rate to use.
     */
    public void setLearningRate(double lr) {
        m_learningRate = lr;
    }

    /**
     * Get the learning rate.
     *
     * @return the learning rate
     */
    public double getLearningRate() {
        return m_learningRate;
    }

    /**
     * Reset the classifier.
     */
    public void reset() {
        m_t = 1;
        m_weights = null;
    }

    protected double dloss(double z) {
        if (m_loss == HINGE) {
            return (z < 1) ? 1 : 0;
        }

        if (m_loss == LOGLOSS) {
            // log loss
            if (z < 0) {
                return 1.0 / (Math.exp(z) + 1.0);
            } else {
                double t = Math.exp(-z);
                return t / (t + 1);
            }
        }

        // squared loss
        return z;
    }

    protected static double dotProd(Instance inst1, double[] weights, int classIndex) {
        double result = 0;

        int n1 = inst1.numValues();
        int n2 = weights.length - 1;

//        for (int p1 = 0, p2 = 0; p1 < n1 && p2 < n2;) {
//            int ind1 = inst1.index(p1);
//            int ind2 = p2;
//            if (ind1 == ind2) {
//                if (ind1 != classIndex && !inst1.isMissingSparse(p1)) {
//                    result += inst1.valueSparse(p1) * weights[p2];
//                }
//                p1++;
//                p2++;
//            } else if (ind1 > ind2) {
//                p2++;
//            } else {
//                p1++;
//            }
//        }

        // simplified wx calculation
        for (int ind1=0;ind1<n2;ind1++){
            if (ind1!=classIndex){
                result+= inst1.value(ind1)*weights[ind1];
            }

        }

        return (result);
    }

    @Override
    public void resetLearningImpl() {
        reset();
        setLambda(this.lambdaRegularizationOption.getValue());
        setLearningRate(this.learningRateOption.getValue());
        setLossFunction(this.lossFunctionOption.getChosenIndex());
    }

    /**
     * Trains the classifier with the given instance.
     *
     * @param instance  the new training instance to include in the model
     */
    @Override
    public void trainOnInstanceImpl(Instance instance) {

        if (m_weights == null) {
            m_weights = new double[instance.numAttributes() + 1];
            try {
                FileReader fr = new FileReader("/Users/wislish/sgdpara.txt");
                BufferedReader br = new BufferedReader(fr);
                String para ="";
                int num=0;
                while((para=br.readLine())!=null){
                    if (num==instance.classIndex()){
                        num++;
                        continue;
                    }
//                    System.out.println(para);
                    m_weights[num]= -(Double.valueOf(para));
                    num++;
                }
//                System.out.println("number of attributes:"+num);

            } catch (IOException e){
                e.printStackTrace();
            }

            m_weights[5] = 1.571394207740338;
//
//            for (int a =0;a<m_weights.length;a++){
//                System.out.println(a+" :"+m_weights[a]);
//            }

        }
//        System.out.println(m_weights[0]);

        if (!instance.classIsMissing()) {

            double wx = dotProd(instance, m_weights, instance.classIndex());

            double y;
            double z;

            if (instance.classAttribute().isNominal()) {
                y = (instance.classValue() == 0) ? -1 : 1;
                z = y * (wx + m_weights[m_weights.length - 1]);
            } else {
                y = instance.classValue();
                z = y - (wx + m_weights[m_weights.length - 1]);
                y = 1;
            }

            // Compute multiplier for weight decay
            double multiplier = 1.0;
            if (m_numInstances == 0) {
                multiplier = 1.0 - (m_learningRate * m_lambda) / m_t;
            } else {
                multiplier = 1.0 - (m_learningRate * m_lambda) / m_numInstances;
            }
            for (int i = 0; i < m_weights.length - 1; i++) {
                m_weights[i] *= multiplier;
            }

            // Only need to do the following if the loss is non-zero
            if (m_loss != HINGE || (z < 1)) {

                // Compute Factor for updates
                double factor = m_learningRate * y * dloss(z);

                // Update coefficients for attributes
                int n1 = instance.numValues();
                for (int p1 = 0; p1 < n1; p1++) {
                    int indS = instance.index(p1);
                    if (indS != instance.classIndex() && !instance.isMissingSparse(p1)) {
                        m_weights[indS] += factor * instance.valueSparse(p1);
                        if (m_t % 3000 ==0){
                            System.out.println(m_weights[indS]);
                        }


                    }
                }
                if (m_t % 3000 ==0) {
                    System.out.println("\n");
                }
                // update the bias
                m_weights[m_weights.length - 1] += factor;
            }
            m_t++;
        }
    }

    /**
     * Calculates the class membership probabilities for the given test
     * instance.
     *
     * @param instance  the instance to be classified
     * @return          predicted class probability distribution
     */
    public double[] getVotesForInstance(Instance inst) {

        if (m_weights == null) {
            return new double[inst.numAttributes() + 1];
        }
        double[] result = (inst.classAttribute().isNominal())
                ? new double[2]
                : new double[1];


        double wx = dotProd(inst, m_weights, inst.classIndex());// * m_wScale;
        double z = (wx + m_weights[m_weights.length - 1]);

        if (inst.classAttribute().isNumeric()) {
            result[0] = z;
            return result;
        }

        if (z <= 0) {
            //  z = 0;
            if (m_loss == LOGLOSS) {
                result[0] = 1.0 / (1.0 + Math.exp(z));
                result[1] = 1.0 - result[0];
            } else {
                result[0] = 1;
            }
        } else {
            if (m_loss == LOGLOSS) {
                result[1] = 1.0 / (1.0 + Math.exp(-z));
                result[0] = 1.0 - result[1];
            } else {
                result[1] = 1;
            }
        }
        return result;
    }

    @Override
    public void getModelDescription(StringBuilder result, int indent) {
        StringUtils.appendIndented(result, indent, toString());
        StringUtils.appendNewline(result);
    }

    /**
     * Prints out the classifier.
     *
     * @return a description of the classifier as a string
     */
    public String toString() {
        if (m_weights == null) {
            return "SGD: No model built yet.\n";
        }
        StringBuffer buff = new StringBuffer();
        buff.append("Loss function: ");
        if (m_loss == HINGE) {
            buff.append("Hinge loss (SVM)\n\n");
        } else if (m_loss == LOGLOSS) {
            buff.append("Log loss (logistic regression)\n\n");
        } else {
            buff.append("Squared loss (linear regression)\n\n");
        }

        // buff.append(m_data.classAttribute().name() + " = \n\n");
        int printed = 0;

        for (int i = 0; i < m_weights.length - 1; i++) {
            // if (i != m_data.classIndex()) {
            if (printed > 0) {
                buff.append(" + ");
            } else {
                buff.append("   ");
            }

            buff.append(Utils.doubleToString(m_weights[i], 12, 4) + " "
                    // + m_data.attribute(i).name()
                    + "\n");

            printed++;
            //}
        }

        if (m_weights[m_weights.length - 1] > 0) {
            buff.append(" + " + Utils.doubleToString(m_weights[m_weights.length - 1], 12, 4));
        } else {
            buff.append(" - " + Utils.doubleToString(-m_weights[m_weights.length - 1], 12, 4));
        }

        return buff.toString();
    }

    @Override
    protected Measurement[] getModelMeasurementsImpl() {
        return null;
    }

    public boolean isRandomizable() {
        return false;
    }
}
