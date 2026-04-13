package me.jaehyeon.hotrolling.domain.model

import java.io.Serializable

class TargetMeanState : Serializable {
    var currentMean: Double = 0.0
    var initialized: Boolean = false

    // Default constructor required by Flink PojoSerializer
    constructor()
}

class SgdState : Serializable {
    var weights: DoubleArray = DoubleArray(0)
    var bias: Double = 0.0
    var initialized: Boolean = false

    constructor()
}

class AmRulesState : Serializable {
    var model: AmRulesModel? = null
    var initialized: Boolean = false

    constructor()
}

class WelfordState : Serializable {
    var count: Long = 0
    var mean: DoubleArray = DoubleArray(0)
    var m2: DoubleArray = DoubleArray(0)

    constructor()
}
