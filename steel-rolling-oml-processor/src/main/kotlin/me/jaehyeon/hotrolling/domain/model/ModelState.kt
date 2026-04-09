package me.jaehyeon.hotrolling.domain.model

import java.io.Serializable

// Flink POJO for TargetMean (EWMA)
class TargetMeanState : Serializable {
    var currentMean: Double = 0.0
    var initialized: Boolean = false

    // Default constructor required by Flink PojoSerializer
    constructor()
}

// Flink POJO for SGD
class SgdState : Serializable {
    var weights: DoubleArray = DoubleArray(0)
    var bias: Double = 0.0
    var initialized: Boolean = false

    constructor()
}
