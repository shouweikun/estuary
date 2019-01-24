package com.neighborhood.aka.laplace.estuary.mysql.schema.def.columndef;

abstract public class EnumeratedColumnDef extends ColumnDef  {

	protected String[] enumValues;

	public EnumeratedColumnDef(String name, String type, int pos, String [] enumValues) {
		super(name, type, pos);
		this.enumValues = enumValues;
	}

	public String[] getEnumValues() {
		return enumValues;
	}
}
