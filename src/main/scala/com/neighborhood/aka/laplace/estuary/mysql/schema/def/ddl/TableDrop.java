package com.neighborhood.aka.laplace.estuary.mysql.schema.def.ddl;

public class TableDrop extends SchemaChange {
	public String database;
	final String table;
	final boolean ifExists;

	public TableDrop(String database, String table, boolean ifExists) {
		this.database = database;
		this.table = table;
		this.ifExists = ifExists;
	}

}
