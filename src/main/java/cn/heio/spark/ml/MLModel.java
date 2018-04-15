package cn.heio.spark.ml;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;

public class MLModel {
    GradientBoostedTreesModel model;
    JavaSparkContext sc;

    public void LoadModel(JavaSparkContext sparkContext, String path){
         model = GradientBoostedTreesModel.load(sc, path);
         sc = sparkContext;
    }

    public void run(String path){
        JavaRDD<String> dataset = sc.textFile(path);
        JavaRDD<LabeledPoint> data = dataset.rdd().map((row) -> {
            LabeledPoint(Double.parseDouble(row[row.size() - 1]), Vectors.dense(row.copy()))});

        // model.predict(data.map(x -> x.features()));
        JavaPairRDD<Double, Double> predictionAndLabel =
                data.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
        double testErr =
                predictionAndLabel.filter(pl -> !pl._1().equals(pl._2())).count() / (double) data.count();
        System.out.println("Test Error: " + testErr);
    }
}
