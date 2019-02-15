package com.neighborhood.aka.laplace.estuary.mysql.schema.defs.columndef;

public abstract class ColumnDefWithDecimalLength extends ColumnDef{

	protected Integer precision;
	protected Integer scale;

	public ColumnDefWithDecimalLength(String name, String type, int pos, Integer precision, Integer scale) {
		super(name, type, pos);
		this.precision = precision;
		this.scale = scale;
	}

	public Integer getPrecision() {
		return precision;
	}

	public void setPrecision(Integer precision) {
		this.precision = precision;
	}

	public Integer getScale() {
		return scale;
	}

	public void setScale(Integer scale) {
		this.scale = scale;
	}
}
