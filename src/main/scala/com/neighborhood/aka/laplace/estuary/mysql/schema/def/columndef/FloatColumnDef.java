package com.neighborhood.aka.laplace.estuary.mysql.schema.def.columndef;

public class FloatColumnDef extends ColumnDef {
	public FloatColumnDef() { }
	public FloatColumnDef(String name, String type, int pos) {
		super(name, type, pos);
	}

	public boolean signed;

	@Override
	public String toSQL(Object value) {
		return value.toString();
	}
}
