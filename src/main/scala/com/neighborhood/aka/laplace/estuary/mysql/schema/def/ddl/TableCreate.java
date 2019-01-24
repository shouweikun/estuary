package com.neighborhood.aka.laplace.estuary.mysql.schema.def.ddl;

import java.util.ArrayList;
import java.util.List;

import com.neighborhood.aka.laplace.estuary.mysql.schema.def.columndef.ColumnDef;

public class TableCreate extends SchemaChange {
	public String database;
	public String table;
	public List<ColumnDef> columns;
	public List<String> pks;
	public String charset;

	public String likeDB;
	public String likeTable;
	public final boolean ifNotExists;

	public TableCreate (String database, String table, boolean ifNotExists) {
		this.database = database;
		this.table = table;
		this.ifNotExists = ifNotExists;
		this.columns = new ArrayList<>();
		this.pks = new ArrayList<>();
	}

}
