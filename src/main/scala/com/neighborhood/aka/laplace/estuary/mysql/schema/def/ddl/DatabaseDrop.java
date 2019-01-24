package com.neighborhood.aka.laplace.estuary.mysql.schema.def.ddl;

public class DatabaseDrop extends SchemaChange {
	public String database;
	public boolean ifExists;

	public DatabaseDrop(String database, boolean ifExists) {
		this.database = database;
		this.ifExists = ifExists;
	}

}
