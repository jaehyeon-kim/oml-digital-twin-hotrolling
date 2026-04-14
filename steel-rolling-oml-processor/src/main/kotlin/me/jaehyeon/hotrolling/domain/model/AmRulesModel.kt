package me.jaehyeon.hotrolling.domain.model

import com.yahoo.labs.samoa.instances.Attribute
import com.yahoo.labs.samoa.instances.DenseInstance
import com.yahoo.labs.samoa.instances.Instance
import com.yahoo.labs.samoa.instances.Instances
import com.yahoo.labs.samoa.instances.InstancesHeader
import moa.classifiers.rules.AMRulesRegressor
import java.io.Serializable

/**
 * A serializable wrapper bridging Flink with the Massive Online Analysis library.
 *
 * AMRules (Adaptive Model Rules) is a state-of-the-art Hoeffding-Tree based regressor that
 * natively detects concept drift via Page-Hinkley tests and prunes or grows rules dynamically.
 *
 * Args:
 * numFeatures: The number of independent variables passed into the model.
 */
class AmRulesModel(
    private val numFeatures: Int,
) : Serializable {
    private var amRules: AMRulesRegressor = AMRulesRegressor()
    private var header: InstancesHeader

    init {
        // Define the attribute schema (Features plus Target) required by the MOA framework
        val attributes = java.util.ArrayList<Attribute>()
        for (i in 0 until numFeatures) {
            attributes.add(Attribute("Feature_$i"))
        }
        attributes.add(Attribute("Target_Residual")) // The physical error we want to predict and correct

        // Create the dataset header. MOA strictly requires Instance objects to be wrapped
        // in an InstancesHeader to understand dimensional boundaries.
        val dataset = Instances("HotRollingDataset", attributes, 0)
        dataset.setClassIndex(numFeatures) // Declare the final attribute as the target
        header = InstancesHeader(dataset)

        // Initialize the Regressor
        amRules.prepareForUse()
        amRules.setModelContext(header)
    }

    /**
     * Executes the Prequential Test-then-Train loop on a single streaming event.
     *
     * In Online Learning, an event must be evaluated BEFORE the model is allowed to train on it.
     * Otherwise, the error metrics are artificially deflated due to data leakage.
     *
     * Args:
     * features: Scaled Z-Scores of the current slab pass.
     * actualResidual: The true physical error (Actual Force minus Baseline Force).
     *
     * Returns:
     * The model prediction of what the residual would be, evaluated prior to training.
     */
    fun predictAndTrain(
        features: DoubleArray,
        actualResidual: Double,
    ): Double {
        // Construct the MOA dense instance
        val instance: Instance = DenseInstance(1.0, DoubleArray(numFeatures + 1))
        instance.setDataset(header)

        for (i in 0 until numFeatures) {
            instance.setValue(i, features[i])
        }

        // The target must be set for the training phase to calculate loss gradients
        instance.setClassValue(actualResidual)

        // PREDICT: Execute the Test phase
        val predictionArray: DoubleArray? = amRules.getVotesForInstance(instance)

        // Extract prediction safely
        val predictedResidual =
            if (predictionArray != null && predictionArray.isNotEmpty()) {
                predictionArray[0]
            } else {
                0.0 // Default to zero adjustment if the model is uninitialized
            }

        // TRAIN: Learn from the true physical residual
        amRules.trainOnInstance(instance)

        return predictedResidual
    }
}
