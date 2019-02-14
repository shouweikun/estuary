package com.neighborhood.aka.laplace.estuary.mysql.schema.defs.columndef;


import com.neighborhood.aka.laplace.estuary.mysql.schema.event.deserialization.json.JsonBinary;
import com.neighborhood.aka.laplace.estuary.mysql.schema.row.RawJSONString;

import java.io.IOException;



public class JsonColumnDef extends ColumnDef {
	public JsonColumnDef(String name, String type, int pos) {
		super(name, type, pos);
	}

	@Override
	public Object asJSON(Object value) {
		try {
			byte[] bytes = (byte[]) value;
			String jsonString = bytes.length > 0 ? JsonBinary.parseAsString(bytes) : "null";
			return new RawJSONString(jsonString);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String toSQL(Object value) {
		return null;
	}
}
