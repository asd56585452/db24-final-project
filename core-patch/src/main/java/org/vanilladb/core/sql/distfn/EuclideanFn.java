package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

import jdk.incubator.vector.*;

public class EuclideanFn extends DistanceFn {

    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
        VectorSpecies<Float> species = FloatVector.SPECIES_256;
        FloatVector result = FloatVector.zero(species);
        float[] resultArray = new float[vec.dimension()];
        for (int i = 0; i < vec.dimension(); i += species.length()) {
            FloatVector a = FloatVector.fromArray(species, query.asJavaVal(), i);
            FloatVector b = FloatVector.fromArray(species, vec.asJavaVal(), i);
            FloatVector diff = a.sub(b);
            result = diff.mul(diff);
            result.intoArray(resultArray, i);
        }

        double sum = 0;
        for (int i = 0; i <  vec.dimension(); i++) {
            sum += resultArray[i];
        }

        return Math.sqrt(sum);
    }
 
}
