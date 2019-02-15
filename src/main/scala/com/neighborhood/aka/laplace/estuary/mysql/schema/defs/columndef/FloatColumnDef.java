package com.neighborhood.aka.laplace.estuary.mysql.schema.defs.columndef;

public class FloatColumnDef extends ColumnDefWithDecimalLength {

	public boolean signed;

	public FloatColumnDef(String name, String type, int pos, Integer precision, Integer scale) {
		super(name, type, pos, precision, scale);
	}

	@Override
	public String toSQL(Object value) {
		return value.toString();
	}
}