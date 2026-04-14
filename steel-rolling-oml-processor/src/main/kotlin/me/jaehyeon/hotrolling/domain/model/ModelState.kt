@file:Suppress("ktlint:standard:kdoc")

package me.jaehyeon.hotrolling.domain.model

import java.io.Serializable

/**
 * Contains the Plain Old Java Objects used by Flink to manage streaming state.
 *
 * To participate in Flink's managed state and checkpointing systems efficiently,
 * a class must adhere to strict POJO rules: it must be public, not final,
 * have a public no-argument default constructor, and all fields must be public
 * or have public getters and setters.
 */

/**
 * State container for the Hybrid Exponentially Weighted Moving Average tracker.
 */
@Suppress("ktlint:standard:no-consecutive-comments")
class TargetMeanState : Serializable {
    var currentMean: Double = 0.0
    var initialized: Boolean = false

    constructor()
}

/**
 * State container for the Stochastic Gradient Descent linear regressor.
 * Holds the continuously updating weights and bias for the model.
 */
class SgdState : Serializable {
    var weights: DoubleArray = DoubleArray(0)
    var bias: Double = 0.0
    var initialized: Boolean = false

    constructor()
}

/**
 * State container for the Welford Online Feature Scaler.
 * Holds the historical count, streaming mean, and sum of squared differences
 * required to calculate true standard deviation on the fly.
 */
class WelfordState : Serializable {
    var count: Long = 0
    var mean: DoubleArray = DoubleArray(0)
    var m2: DoubleArray = DoubleArray(0)

    constructor()
}
