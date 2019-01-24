package com.neighborhood.aka.laplace.estuary.mysql.schema.def.ddl;

public class DatabaseCreate extends SchemaChange {
	public final String database;
	private final boolean ifNotExists;
	public final String charset;

	public DatabaseCreate(String database, boolean ifNotExists, String charset) {
		this.database = database;
		this.ifNotExists = ifNotExists;
		this.charset = charset;
	}

}
