package com.neighborhood.aka.laplace.estuary.mysql.schema.def.ddl;

import com.neighborhood.aka.laplace.estuary.mysql.schema.def.columndef.ColumnDef;

class ChangeColumnMod extends ColumnMod {
	public ColumnDef definition;
	public ColumnPosition position;

	public ChangeColumnMod(String name, ColumnDef d, ColumnPosition position ) {
		super(name);
		this.definition = d;
		this.position = position;
	}

}

