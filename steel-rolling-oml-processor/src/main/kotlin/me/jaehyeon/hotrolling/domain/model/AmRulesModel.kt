package me.jaehyeon.hotrolling.domain.model

import com.yahoo.labs.samoa.instances.Attribute
import com.yahoo.labs.samoa.instances.DenseInstance
import com.yahoo.labs.samoa.instances.Instance
import com.yahoo.labs.samoa.instances.Instances
import com.yahoo.labs.samoa.instances.InstancesHeader
import moa.classifiers.rules.AMRulesRegressor
import java.io.Serializable

class AmRulesModel(
    private val numFeatures: Int,
) : Serializable {
    private var amRules: AMRulesRegressor = AMRulesRegressor()
    private var header: InstancesHeader

    init {
        // 1. Define the attributes (Features + Target)
        val attributes = java.util.ArrayList<Attribute>()
        for (i in 0 until numFeatures) {
            attributes.add(Attribute("Feature_$i"))
        }
        attributes.add(Attribute("Target_Residual")) // The error we want to predict

        // 2. Create the dataset header for MOA
        // MOA strictly requires Instances to be wrapped in an InstancesHeader
        val dataset = Instances("HotRollingDataset", attributes, 0)
        dataset.setClassIndex(numFeatures) // The last attribute is the target
        header = InstancesHeader(dataset)

        // 3. Configure AMRules Hyperparameters
        // AMRulesRegressor natively uses Page-Hinkley for drift detection by default
        amRules.prepareForUse()
        amRules.setModelContext(header)
    }

    fun predictAndTrain(
        features: DoubleArray,
        actualResidual: Double,
    ): Double {
        // 1. Create a MOA Instance from our Welford-scaled features
        // We explicitly cast to the `Instance` interface to expose setDataset and setClassValue.
        // DenseInstance requires a weight (1.0) and a DoubleArray of values.
        val instance: Instance = DenseInstance(1.0, DoubleArray(numFeatures + 1))
        instance.setDataset(header)

        for (i in 0 until numFeatures) {
            instance.setValue(i, features[i])
        }

        // Set the actual target (what the model SHOULD have predicted)
        instance.setClassValue(actualResidual)

        // 2. Predict the residual BEFORE training (Pre-quential evaluation)
        // Kotlin interprets MOA's double[] return type as a nullable DoubleArray?
        val predictionArray: DoubleArray? = amRules.getVotesForInstance(instance)

        // Safely extract the prediction, defaulting to 0.0 if empty/null
        val predictedResidual =
            if (predictionArray != null && predictionArray.isNotEmpty()) {
                predictionArray[0]
            } else {
                0.0
            }

        // 3. Train the model on the true physical residual
        amRules.trainOnInstance(instance)

        return predictedResidual
    }
}
