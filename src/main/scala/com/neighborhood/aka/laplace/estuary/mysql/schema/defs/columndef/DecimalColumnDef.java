package com.neighborhood.aka.laplace.estuary.mysql.schema.defs.columndef;

import java.math.BigDecimal;

public class DecimalColumnDef extends ColumnDefWithDecimalLength {

    public DecimalColumnDef(String name, String type, int pos, Integer precision, Integer scale) {
        super(name, type, pos, precision, scale);
    }

    @Override
    public String toSQL(Object value) {
        BigDecimal d = (BigDecimal) value;

        return d.toEngineeringString();
    }
}
