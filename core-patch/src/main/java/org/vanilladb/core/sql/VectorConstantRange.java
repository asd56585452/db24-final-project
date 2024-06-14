package org.vanilladb.core.sql;

public class VectorConstantRange extends ConstantRange{
    private VectorConstant low;
	private VectorConstant high;
	private boolean lowIncl;
	private boolean highIncl;

	/**
	 * Constructs a new instance.
	 * 
	 * @param low
	 *            the lower bound. <code>null</code> means unbound.
	 * @param lowIncl
	 *            whether the lower bound is inclusive
	 * @param high
	 *            the higher bound. <code>null</code> means unbound.
	 * @param highIncl
	 *            whether the higher bound is inclusive
	 */

     VectorConstantRange(VectorConstant low, boolean lowIncl,
        VectorConstant high, boolean highIncl) {
        this.low = low;
        this.lowIncl = lowIncl;
        this.high = high;
        this.highIncl = highIncl;
	}

	/*
	 * Getters
	 */

	@Override
	public boolean isValid() {
        if(low!=null && high!=null && lowIncl && highIncl){
            return low.equals(high);
        }
        return false;
	}

	@Override
	public boolean hasLowerBound() {
		return low != null;
	}

	@Override
	public boolean hasUpperBound() {
		return high != null;
	}

	@Override
	public Constant low() {
		if (low!=null)
			return low;
		throw new IllegalStateException();
	}

	@Override
	public Constant high() {
		if (high!=null)
			return high;
		throw new IllegalStateException();
	}

	@Override
	public boolean isLowInclusive() {
		return lowIncl;
	}

	@Override
	public boolean isHighInclusive() {
		return highIncl;
	}

	@Override
	public double length() {
		throw new UnsupportedOperationException();
	}

	/*
	 * Constant operations.
	 */

	@Override
	public ConstantRange applyLow(Constant c, boolean incl) {
		if (!(c instanceof VectorConstant))
			throw new IllegalArgumentException();
        if (!c.equals(low))
            throw new IllegalArgumentException();
		return new VectorConstantRange(low, lowIncl, high, highIncl);
	}

	@Override
	public ConstantRange applyHigh(Constant c, boolean incl) {
		if (!(c instanceof VectorConstant))
			throw new IllegalArgumentException();
        if (!c.equals(high))
            throw new IllegalArgumentException();
		return new VectorConstantRange(low, lowIncl, high, highIncl);
	}

	@Override
	public ConstantRange applyConstant(Constant c) {
		if (!(c instanceof VectorConstant))
			throw new IllegalArgumentException();
		return applyLow(c, true).applyHigh(c, true);
	}

	@Override
	public boolean isConstant() {
		// do not use !NINF.equals(low), if low = "", this may goes wrong
		return low.equals(high) && lowIncl == true && highIncl == true;
	}

	@Override
	public Constant asConstant() {
		if (isConstant())
			return low;
		throw new IllegalStateException();
	}

	@Override
	public boolean contains(Constant c) {
		if (!(c instanceof VectorConstant))
			throw new IllegalArgumentException();
		if (!isValid())
			return false;
		/*
		 * Note that if low and high are INF ore NEG_INF here, using
		 * c.compare(high/low) will have wrong answer.
		 * 
		 * For example, if high=INF, the result of c.compareTo(high) is the same
		 * as c.compareTo("Infinity").
		 */
		if (c.equals(low))
		    return true;
        else
            return false;
	}

	@Override
	public boolean lessThan(Constant c) {
		if (low.equals(c))
			return false;
        throw new IllegalArgumentException();
	}

	@Override
	public boolean largerThan(Constant c) {
		if (low.equals(c))
			return false;
        throw new IllegalArgumentException();
	}

	/*
	 * Range operations.
	 */

	@Override
	public boolean isOverlapping(ConstantRange r) {
		if (!(r instanceof VectorConstantRange))
			throw new IllegalArgumentException();
		if (!isValid() || !r.isValid())
			return false;
        if(r.low().equals(low))
		    return true;
        else
            return false;
	}

	// TODO : check the possible of c.compareTo(INF)
	@Override
	public boolean contains(ConstantRange r) {
		if (!(r instanceof VectorConstantRange))
			throw new IllegalArgumentException();
		if (!isValid() || !r.isValid())
			return false;
        if(r.low().equals(low))
		    return true;
        else
            return false;
	}

	@Override
	public ConstantRange intersect(ConstantRange r) {
		if (!(r instanceof VectorConstantRange))
			throw new IllegalArgumentException();
        if(r.low().equals(low) && isValid() && r.isValid())
		    return new VectorConstantRange(low, lowIncl, high, highIncl);
        else
            return new VectorConstantRange(null, lowIncl, null, highIncl);
	}

	@Override
	public ConstantRange union(ConstantRange r) {
		if (!(r instanceof VectorConstantRange))
			throw new IllegalArgumentException();
        if(r.low().equals(low) && isValid() && r.isValid())
		    return new VectorConstantRange(low, lowIncl, high, highIncl);
        else
            return new VectorConstantRange(null, lowIncl, null, highIncl);
	}
}
