package com.brofan.service.classifier.sgd;

import java.io.IOException;

import com.brofan.table.ShopFeatureTable;
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
import org.apache.mahout.classifier.sgd.AdaptiveLogisticRegression.Wrapper;
import org.apache.mahout.ep.State;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.brofan.service.classifier.sgd.entity.ShopEncoder;
import com.brofan.service.classifier.sgd.entity.ShopFeatures;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
import de.bwaldvogel.liblinear.Parameter;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.SolverType;

public class TrainShopModel extends Configured {

	private ShopHelper helper = new ShopHelper();
	private ShopHelper helper2 = new ShopHelper();
	public void train() throws IOException {

		AdaptiveLogisticRegression learningAlgorithm =
				new AdaptiveLogisticRegression(helper.getCategoriesCount(), helper.getFeaturesCount(), new L1());
		learningAlgorithm.setInterval(800);
		learningAlgorithm.setAveragingWindow(500);

		ShopEncoder encoder = new ShopEncoder();
		CrossFoldLearner model = null;
		State<Wrapper, CrossFoldLearner> best;

		int k = 0;
		Vector vector = new RandomAccessSparseVector(helper.getFeaturesCount());
		while (helper.hasNext()) {
			ShopFeatures sf = helper.getShopFeatures();
			encoder.addToVector(sf, vector);
			learningAlgorithm.train(sf.hasSpam(), vector);

			k++;
			best = learningAlgorithm.getBest();

			if (best != null) {
				model = best.getPayload().getLearner();

				if (model != null) {
					double averageCorrect = model.percentCorrect();
					double averageLL = model.logLikelihood();

					System.out.printf("%d\t%.3f\t%.2f\n",
							k, averageLL, averageCorrect * 100);
				}
			}
		}
		learningAlgorithm.close();

		best = learningAlgorithm.getBest();
		if (best != null) {
			model = best.getPayload().getLearner();
		}
		if (model == null) {
			System.out.println("AdaptiveLogisticRegression has failed to train a model.");
			return;
		}

		System.out.println("AUC=" + model.auc() + ", %-correct=" + model.percentCorrect());
		String path = getConf().get("model.shop.path");
		System.out.println("Writing model to " + path);
		ModelSerializer.writeBinary(path,
				learningAlgorithm.getBest().getPayload().getLearner());
		System.out.println("Finished!");
	}

	public void trainWithLibLinear() throws IOException {
		int datasize = (int)helper.size();
//		int datasize = 109055;
		System.out.println("Shop Total Size: " + datasize);

		SolverType solver = SolverType.L2R_LR; // -s 0
		double C = 1.0; // cost of constraints violation
		double eps = 0.01;
		double correct = 0;
		double[] probs = new double[datasize];
		double[] prob = new double[2];
		Problem problem = new Problem();
		problem.n = 7;
		problem.l = datasize;
		problem.bias = 1;
		int k = 0;
		double[] target = new double[datasize];
		Feature[][] shopfeatures = new Feature[datasize][7];
		while (helper.hasNext()) {

			ShopFeatures sf = helper.getShopFeatures();
			//System.out.println(sf.getScore2());
			FeatureNode sr = new FeatureNode(1, sf.getSR());
			FeatureNode rcv = new FeatureNode(2, sf.getRCV());
			FeatureNode star = new FeatureNode(3, sf.getStar());
			FeatureNode score1 = new FeatureNode(4, sf.getScore1());
			FeatureNode score2 = new FeatureNode(5, sf.getScore2());
			FeatureNode score3 = new FeatureNode(6, sf.getScore3());

			FeatureNode bias = new FeatureNode(7, 1);
			shopfeatures[k][0] = sr;
			shopfeatures[k][1] = rcv;
			shopfeatures[k][2] = star;
			shopfeatures[k][3] = score1;
			shopfeatures[k][4] = score2;
			shopfeatures[k][5] = score3;
			shopfeatures[k][6] = bias;

			target[k] = sf.hasSpam();

			k++;
		}
		problem.x = shopfeatures;
		problem.y = target;

		Parameter parameter = new Parameter(solver, C, eps);
		Model model = Linear.train(problem, parameter);
		// File modelFile = new File("home/wislish/model/");
		// model.save(modelFile);
		// // load model or use it directly
		// model = Model.load(modelFile);
		int num = 0;
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "sFeature");

		while (helper2.hasNext()) {
			ShopFeatures sf = helper2.getShopFeatures();
			FeatureNode sr = new FeatureNode(1, sf.getSR());
			FeatureNode rcv = new FeatureNode(2, sf.getRCV());
			FeatureNode star = new FeatureNode(3, sf.getStar());
			FeatureNode score1 = new FeatureNode(4, sf.getScore1());
			FeatureNode score2 = new FeatureNode(5, sf.getScore2());
			FeatureNode score3 = new FeatureNode(6, sf.getScore3());
			FeatureNode bias = new FeatureNode(7, 1);
			Feature[] instance = { sr, rcv, star, score1, score2,score3,bias };
			double prediction = Linear.predict(model, instance);
			Linear.predictProbability(model, instance, prob);
			probs[num] = prob[1];

			if (((int) prediction) == sf.hasSpam()) {
				correct++;
			}

			Put put = new Put(Bytes.toBytes(sf.getSid()));
			put.add(ShopFeatureTable.FAM_NAME, ShopFeatureTable.CREDIBILITY_COL, Bytes.toBytes(prob[1]));
			table.put(put);

			System.out.println(sf.hasSpam() + " " + prediction + " " + correct);
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

		TrainShopModel tsm = new TrainShopModel();
		tsm.setConf(conf);
//		tsm.train();
		tsm.trainWithLibLinear();
	}
}