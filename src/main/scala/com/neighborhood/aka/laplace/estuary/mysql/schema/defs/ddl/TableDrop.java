package com.neighborhood.aka.laplace.estuary.mysql.schema.defs.ddl;

public class TableDrop extends SchemaChange {
	public String database;
	public final  String table;
	public final boolean ifExists;

	public TableDrop(String database, String table, boolean ifExists) {
		this.database = database;
		this.table = table;
		this.ifExists = ifExists;
	}

}
