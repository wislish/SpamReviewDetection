package com.brofan.service.classifier.sgd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.mahout.classifier.evaluation.Auc;
import org.apache.mahout.classifier.sgd.AdaptiveLogisticRegression;
import org.apache.mahout.classifier.sgd.CrossFoldLearner;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.ModelSerializer;
import org.apache.mahout.ep.State;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.brofan.service.classifier.sgd.entity.UserEncoder;
import com.brofan.service.classifier.sgd.entity.UserFeatures;
import com.brofan.table.UserFeatureTable;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
import de.bwaldvogel.liblinear.Parameter;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.SolverType;

public class TrainUserModel extends Configured {

    private UserHelper helper = new UserHelper();
    private UserHelper helper2 = new UserHelper();
    private int datasize;

    public void train() throws IOException {

        CrossFoldLearner model = null;
        State<AdaptiveLogisticRegression.Wrapper, CrossFoldLearner> best;
        AdaptiveLogisticRegression learningAlgorithm = new AdaptiveLogisticRegression(
                helper.getCategoriesCount(), helper.getFeaturesCount(),
                new L1());
        learningAlgorithm.setInterval(800);
        learningAlgorithm.setAveragingWindow(500);

        UserEncoder encoder = new UserEncoder();

        int k = 0;

        while (helper.hasNext()) {
            Vector vector = new RandomAccessSparseVector(
                    helper.getFeaturesCount());
            UserFeatures uf = helper.getUserFeatures();
            encoder.addToVector(uf, vector);
            learningAlgorithm.train(uf.isSpam(), vector);

            k++;
            best = learningAlgorithm.getBest();

            if (best != null && k % 1000 == 0) {
                model = best.getPayload().getLearner();

                double averageCorrect = model.percentCorrect();
                double averageLL = model.logLikelihood();

                System.out.printf("%d\t%.3f\t%.2f%n", k, averageLL,
                        averageCorrect * 100);
            }
        }
        learningAlgorithm.close();

        best = learningAlgorithm.getBest();
        if (best != null) {
            model = best.getPayload().getLearner();
        }
        if (model == null) {
            System.out
                    .println("AdaptiveLogisticRegression has failed to train a model.");
            return;
        }
        System.out.println("AUC=" + model.auc() + ", %-correct="
                + model.percentCorrect());
        String path = getConf().get("model.user.path");
        System.out.println("Writing model to " + path);
        ModelSerializer.writeBinary(path, learningAlgorithm.getBest()
                .getPayload().getLearner().getModels().get(0));
        System.out.println("Finished!");
        System.out.println("Test begins!");
        // OnlineLogisticRegression classifier = ModelSerializer.readBinary(
        // new FileInputStream(path), OnlineLogisticRegression.class);
        // Vector test_result = new
        // RandomAccessSparseVector(helper2.getFeaturesCount());
        // while (helper2.hasNext()) {
        // UserFeatures uf_test = helper2.getUserFeatures();
        // encoder.addToVector(uf_test, test_result);
        // Vector result = classifier.classifyFull(test_result);
        // result.maxValueIndex();
        // }
    }

    public void trainWithLibLinear() throws IOException {
        datasize = (int)helper.size();
//		datasize = 237535;
        System.out.println("User Total Size: " + datasize);

        SolverType solver = SolverType.L2R_LR; // -s 0
        double C = 1.0; // cost of constraints violation
        double eps = 0.01;
        double correct = 0;
        double[] probs = new double[datasize];
        double[] prob = new double[2];
        Problem problem = new Problem();
        problem.n = 5;
        problem.l = datasize;
        problem.bias = 1;
        int k = 0;
        double[] target = new double[datasize];
        Feature[][] userfeatures = new Feature[datasize][5];
        while (helper.hasNext()) {

            UserFeatures uf = helper.getUserFeatures();
            FeatureNode rd = new FeatureNode(1, uf.getRD());
            FeatureNode c = new FeatureNode(2, uf.getC());
            FeatureNode etf = new FeatureNode(3, uf.getETF());
            FeatureNode sr = new FeatureNode(4, uf.isSR() ? 1 : 0);
            FeatureNode bias = new FeatureNode(5, 1);
            userfeatures[k][0] = rd;
            userfeatures[k][1] = c;
            userfeatures[k][2] = etf;
            userfeatures[k][3] = sr;
            userfeatures[k][4] = bias;

            target[k] = uf.isSpam();

            k++;
        }
        problem.x = userfeatures;
        problem.y = target;

        Parameter parameter = new Parameter(solver, C, eps);
        Model model = Linear.train(problem, parameter);
        // File modelFile = new File("home/wislish/model/");
        // model.save(modelFile);
        // // load model or use it directly
        // model = Model.load(modelFile);
        int num = 0;
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "uFeature");

        while (helper2.hasNext()) {
            UserFeatures uf = helper2.getUserFeatures();
            FeatureNode rd = new FeatureNode(1, uf.getRD());
            FeatureNode c = new FeatureNode(2, uf.getC());
            FeatureNode etf = new FeatureNode(3, uf.getETF());
            FeatureNode sr = new FeatureNode(4, uf.isSR() ? 1 : 0);
            FeatureNode bias = new FeatureNode(5, 1);
            Feature[] instance = { rd, c, etf, sr, bias };
            double prediction = Linear.predict(model, instance);
            Linear.predictProbability(model, instance, prob);
            probs[num] = prob[1];

            if (((int) prediction) == uf.isSpam()) {
                correct++;
            }

            Put put = new Put(Bytes.toBytes(uf.getUid()));
            put.add(UserFeatureTable.FAM_NAME, UserFeatureTable.CREDIBILITY_COL,Bytes.toBytes(prob[1]));
            table.put(put);

            System.out.println(uf.isSpam() + " " + prediction + " " + correct);
            num++;
        }
        Auc auc = new Auc();
        auc.setMaxBufferSize(datasize);
        for (int i = 0; i < probs.length; i++) {
            auc.add((int) (target[i]), probs[i]);
        }


        System.out.println("Finished! " + correct / datasize);
        System.out.println("AUC IS : " + auc.auc());
    }

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.addResource("project-site.xml");

        TrainUserModel tum = new TrainUserModel();
        tum.setConf(conf);
        // tum.train();
        tum.trainWithLibLinear();
    }
}