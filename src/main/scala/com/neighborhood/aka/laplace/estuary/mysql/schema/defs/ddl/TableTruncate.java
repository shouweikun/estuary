package com.neighborhood.aka.laplace.estuary.mysql.schema.defs.ddl;

public class TableTruncate extends SchemaChange {
	public String database;
	final String table;

	public TableTruncate(String database, String table) {
		this.database = database;
		this.table = table;
	}

}
