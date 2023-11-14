package uk.co.devworx.xmlflattener;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class Logger
{
    public static final boolean DEBUG = false;


    public static Logger getLogger(Class<?> clazz)
    {
        return new Logger(clazz);
    }

    private Class<?> clazz;

    private Logger(Class<?> clazz)
    {
        this.clazz = clazz;
    }

    public boolean isDebugEnabled()
    {
        return DEBUG;
    }

    public void debug(String s)
    {
        if(!isDebugEnabled()) return;
        String val = String.format("%s - [DEBUG] - %s", Instant.now(), s);
        System.out.println(val);
    }

    public void debug(Exception e)
    {
        debug(String.valueOf(e));
    }

    public void info(String s)
    {
        String val = String.format("%s - [INFO] - %s", Instant.now(), s);
        System.out.println(val);
    }

    public void warn(String s)
    {
        String val = String.format("%s - [WARN] - %s", Instant.now(), s);
        System.out.println(val);
    }

    public void warn(String s, Exception ex)
    {
        String exception = makeString(ex);
        String val = String.format("%s - [WARN] - %s | %s", Instant.now(), s, exception);
        System.out.println(val);
    }

    public void error(String msg, Throwable e)
    {
        String exception = makeString(e);
        String val = String.format("%s - [ERROR] - %s | %s", Instant.now(), msg, exception);
        System.out.println(val);
    }

    private String makeString(Throwable ex)
    {
        ByteArrayOutputStream bous = new ByteArrayOutputStream();
        PrintStream prs = new PrintStream(bous);
        ex.printStackTrace(prs);
        prs.close();
        return new String(bous.toByteArray(), StandardCharsets.UTF_8);
    }


    public void info(Object obj)
    {
        info(String.valueOf(obj));
    }
}
