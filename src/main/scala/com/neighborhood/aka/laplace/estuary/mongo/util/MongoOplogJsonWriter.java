package com.neighborhood.aka.laplace.estuary.mongo.util;

import org.bson.BSONException;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.Writer;
import java.lang.reflect.Method;

/**
 * Created by duming on 17/1/15.
 */
public class MongoOplogJsonWriter extends JsonWriter {


    public MongoOplogJsonWriter(Writer writer) {
        super(writer);
    }

    public MongoOplogJsonWriter(Writer writer, JsonWriterSettings settings) {
        super(writer, settings);
    }

    @Override
    protected void doWriteDateTime(long value) {
        try {
            writeNameHelper(getName());
            getWriter().write(Long.toString(value));
        } catch (Exception e) {
            throwBSONException(e);
        }
    }

    @Override
    protected void doWriteInt64(long value) {
        try {
            writeNameHelper(getName());
            getWriter().write(Long.toString(value));
        } catch (Exception e) {
            throwBSONException(e);
        }
    }

    /**
     * 因为JsonWriter中的方法是私有的, 如果拷贝过来要拷贝的类太多了.
     */
    private Method writeNameHelperMethod;

    {
        try {
            writeNameHelperMethod = JsonWriter.class.getDeclaredMethod("writeNameHelper", new Class[]{String.class});
            writeNameHelperMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeNameHelper(final String name) throws Exception {
        writeNameHelperMethod.invoke(this, name);
    }

    private void throwBSONException(final Exception e) {
        throw new BSONException("Wrapping IOException", e);
    }

}
