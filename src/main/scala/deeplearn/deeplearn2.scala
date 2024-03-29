/*import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.split.FileSplit;
import org.datavec.api.util.ClassPathResource;
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.SplitTestAndTrain;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.dataset.api.preprocessor.DataNormalization;
import org.nd4j.linalg.dataset.api.preprocessor.NormalizerStandardize;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File

object deeplearn2 extends App{

        //First: get the dataset using the record reader. CSVval handles loading/parsing
        val numLinesToSkip = 0;
        val delimiter = '\t';
        val recordReader = new CSVRecordReader(numLinesToSkip,delimiter);
        recordReader.initialize(new FileSplit(
           new File("/data/BigData/admissions/AdmissionAnon3.tsv") 
        ))

        //Second: the valvalIterator handles conversion to val objects, ready for use in neural network
        val labelIndex = 40;     //5 values in each row of the iris.txt CSV: 4 input features followed by an valeger label (class) index. Labels are the 5th value (index 4) in each row
        val numClasses = 2;     //3 classes (types of iris flowers) in the iris data set. Classes have valeger values 0, 1 or 2
        val batchSize = 150;    //Iris data set: 150 examples total. We are loading all of them valo one val (not recommended for large data sets)

        val iterator = new RecordReaderDataSetIterator(recordReader,batchSize,labelIndex,numClasses);
        val allData = iterator.next();
        allData.shuffle();
        val testAndTrain = allData.splitTestAndTrain(0.50);  //Use 65% of data for training

        val trainingData = testAndTrain.getTrain();
        val testData = testAndTrain.getTest();

        //We need to normalize our data. We'll use NormalizeStandardize (which gives us mean 0, unit variance):
        val normalizer = new NormalizerStandardize();
        normalizer.fit(trainingData);           //Collect the statistics (mean/stdev) from the training data. This does not modify the input data
        normalizer.transform(trainingData);     //Apply normalization to the training data
        normalizer.transform(testData);         //Apply normalization to the test data. This is using statistics calculated from the *training* set


        val numInputs = 40;
        val outputNum = 2;
        val iterations = 3000;
        val seed = 6;


        println("Build model....");
        val conf = new NeuralNetConfiguration.Builder()
            .seed(seed)
            .iterations(iterations)
            .activation(Activation.SIGMOID)
            .weightInit(WeightInit.XAVIER)
            .learningRate(0.1)
            .regularization(true).l2(1e-4)
            .list()
            .layer(0, new DenseLayer.Builder().nIn(numInputs).nOut(3)
                .build())
            .layer(1, new DenseLayer.Builder().nIn(3).nOut(3)
                .build())
            .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
                .activation(Activation.SOFTMAX)
                .nIn(3).nOut(outputNum).build())
            .backprop(true).pretrain(false)
            .build();

        //run the model
        val model = new MultiLayerNetwork(conf);
        model.init();
        model.setListeners(new ScoreIterationListener(100));

        model.fit(trainingData);

        //evaluate the model on the test set
        val eval = new Evaluation(2);
        val output = model.output(testData.getFeatureMatrix());
        eval.eval(testData.getLabels(), output);
        println(eval.stats());

}
*/
