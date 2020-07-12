package uk.co.devworx.xmlflattener;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;


import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Parameter bag is a class that contains extra items that could be referenced by some of the column definitions
 * e.g. the batchtime
 */

public class ParameterBag
{
    static final DocumentBuilderFactory docFactory;
    static final DocumentBuilder builder;
    static final CachingContextClassLoader contextClassLoader;
    public static final boolean USE_STRATEGY_1_FOR_XML_DOC = Boolean.valueOf(System.getProperty("uk.co.devworx.xmlflattener.XmlExtractor.uk.co.devworx.xmlflattener.ParameterBag.Strategy_1", "false"));

    static
    {
        try
        {
            docFactory = DocumentBuilderFactory.newInstance();
            builder = docFactory.newDocumentBuilder();
            contextClassLoader = new CachingContextClassLoader(ClassLoader.getSystemClassLoader());
        } catch (Exception e)
        {
            throw new RuntimeException("Could not create the XML parsing libraries - something is wrong with your setup : " + e);
        }
    }

    public static ParameterBag create(byte[] documentData, Timestamp batchtime, Map<String, String> sqlValues) throws SAXException, IOException
    {
        return new ParameterBag(documentData, batchtime, sqlValues);
    }

    public static final String BATCH_TIME_COLNAME = "batchTime";
    public static final String CURRENT_TIME_COLNAME = "currentTime";

    private final Timestamp batchtime;
    private final Map<String, String> sqlValues;
    private final Document xmlDoc;
    private final ReentrantLock xmlDocCloneLock = new ReentrantLock();
    private final ThreadLocal<Document> xmlDocThreadHolder = new ThreadLocal<>();
    private final ThreadLocal<DocumentBuilder> documentBuilderThreadLocal = new ThreadLocal<>();
    private final byte[] documentData;

    private ParameterBag(byte[] documentData, Timestamp batchtime, Map<String, String> sqlValues) throws SAXException, IOException
    {
        this.batchtime = batchtime;
        this.sqlValues = Collections.unmodifiableMap(new ConcurrentHashMap<>(sqlValues));
        this.documentData = documentData;
        xmlDoc = builder.parse(new ByteArrayInputStream(documentData));
    }

    public byte[] getDocumentData()
    {
        return documentData;
    }

    public Timestamp getBatchtime()
    {
        return batchtime;
    }

    public Map<String, String> getSqlValues()
    {
        return sqlValues;
    }

    public Document getThreadSafeXmlDoc()
    {
        if (USE_STRATEGY_1_FOR_XML_DOC == true)
        {
            return ___getThreadSafeXmlDoc_strategy_1();
        } else
        {
            return ___getThreadSafeXmlDoc_strategy_2();
        }
    }

    private Document ___getThreadSafeXmlDoc_strategy_1()
    {
        Document xmlDocInThread = xmlDocThreadHolder.get();
        if (xmlDocInThread != null)
        {
            return xmlDocInThread;
        }
        try
        {
            xmlDocCloneLock.lock();
            xmlDocInThread = (Document) xmlDoc.cloneNode(true);
            xmlDocThreadHolder.set(xmlDocInThread);
            return xmlDocInThread;
        } catch (Exception e)
        {
            throw new RuntimeException("Unable to clone / parse the document : " + e, e);
        } finally
        {
            xmlDocCloneLock.unlock();
        }
    }

    private Document ___getThreadSafeXmlDoc_strategy_2()
    {
        Document xmlDocInThread = xmlDocThreadHolder.get();
        if (xmlDocInThread != null)
        {
            return xmlDocInThread;
        }
        try
        {
            final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if ((classLoader == contextClassLoader) == false)
            {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
            xmlDocInThread = getThreadSafeDocumentBuilder().parse(new ByteArrayInputStream(documentData));
            xmlDocThreadHolder.set(xmlDocInThread);
            return xmlDocInThread;
        } catch (Exception e)
        {
            throw new RuntimeException("Unable to clone / parse the document : " + e, e);
        }
    }

    private DocumentBuilder getThreadSafeDocumentBuilder()
    {
        DocumentBuilder docBuilder = documentBuilderThreadLocal.get();
        if (docBuilder != null)
        {
            return docBuilder;
        }
        try
        {
            docBuilder = docFactory.newDocumentBuilder();
            documentBuilderThreadLocal.set(docBuilder);
            return docBuilder;
        } catch (Exception e)
        {
            throw new RuntimeException("Could not create the XML parsing libraries - something must be wrong with your setup : " + e, e);
        }
    }
}

class CachingContextClassLoader extends ClassLoader
{
    private final ClassLoader delegate;
    private final ConcurrentHashMap<String, Class<?>> clazzCache;

    public CachingContextClassLoader(ClassLoader delegate)
    {
        super(delegate);
        this.delegate = delegate;
        clazzCache = new ConcurrentHashMap<>();
    }

    @Override
    protected Class<?> findClass(String className) throws ClassNotFoundException
    {
        Class<?> prev = clazzCache.get(className);
        if (prev != null) return prev;
        prev = super.findClass(className);
        clazzCache.put(className, prev);
        return prev;
    }

    @Override
    public Class<?> loadClass(String className) throws ClassNotFoundException
    {
        Class<?> prev = clazzCache.get(className);
        if (prev != null) return prev;
        prev = super.loadClass(className);
        clazzCache.put(className, prev);
        return prev;
    }

    @Override
    protected Class<?> loadClass(String className, boolean resolveClass) throws ClassNotFoundException
    {
        Class<?> prev = clazzCache.get(className);
        if (prev != null) return prev;
        prev = super.loadClass(className, resolveClass);
        clazzCache.put(className, prev);
        return prev;
    }

}
