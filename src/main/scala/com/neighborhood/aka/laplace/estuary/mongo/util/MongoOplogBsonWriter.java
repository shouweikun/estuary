package com.neighborhood.aka.laplace.estuary.mongo.util;

import org.bson.BSONException;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.Writer;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by duming on 17/1/15.
 */
public class MongoOplogBsonWriter extends JsonWriter {

    private SimpleDateFormat sdf = new SimpleDateFormat("\""+"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"+"\"");
    {
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    public MongoOplogBsonWriter(Writer writer) {
        super(writer);
    }
    /* try {
         switch (settings.getOutputMode()) {
             case STRICT:
                 writeStartDocument();
                 writeNameHelper("$date");
                 writer.write(Long.toString(value));
                 writeEndDocument();
                 break;
             case SHELL:
                 writeNameHelper(getName());

                 SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'");
                 dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                 if (value >= -59014396800000L && value <= 253399536000000L) {
                     writer.write(format("ISODate(\"%s\")", dateFormat.format(new Date(value))));
                 } else {
                     writer.write(format("new Date(%d)", value));
                 }
                 break;
             default:
                 throw new BSONException("Unexpected JSONMode.");
         }
     } catch (IOException e) {
         throwBSONException(e);
     }*/
    public MongoOplogBsonWriter(Writer writer, JsonWriterSettings settings) {
        super(writer, settings);
    }

    @Override
    protected void doWriteDateTime(long value) {
        try {
            this.writeStartDocument();
            writeNameHelper("$date");
            Date date=new Date(value);
            String result =sdf.format(date);
            getWriter().write(result);
            this.writeEndDocument();
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
