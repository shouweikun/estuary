package com.neighborhood.aka.laplace.estuary.mysql.schema.def.ddl;

public class DatabaseAlter extends SchemaChange {
	public String database;
	public String charset;

	public DatabaseAlter(String database) {
		this.database = database;
	}

}
