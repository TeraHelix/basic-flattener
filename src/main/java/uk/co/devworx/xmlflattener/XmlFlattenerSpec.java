package uk.co.devworx.xmlflattener;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.w3c.dom.*;

import javax.xml.xpath.*;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A specification for how how the XML structure should be flattened to a tabular one
 */
public class XmlFlattenerSpec
{
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(XmlFlattenerSpec.class);

    static final XPathFactory xPathFactory = XPathFactory.newInstance();
    static final XPath xPath = xPathFactory.newXPath();

    private final String name;
    private final Path originatingFile;
    private final Path inputPath;

    private final Map<String, FlattenerListItem> specListItems;

    private final AtomicLong totalBytesProcessed;
    private final AtomicLong totalBytesToXmlDocConversionDuration;
    private final AtomicLong totalXmlsProcessed;
    private final AtomicLong totalXmlConversionDuration = new AtomicLong();

    XmlFlattenerSpec(Path originatingFile,
                     String name,
                     Map<String, FlattenerListItem> mapListItemsP,
                     Path inputPath)
    {
        this.originatingFile = originatingFile;
        this.name = name;
        this.specListItems = mapListItemsP;
        this.inputPath = inputPath;
        specListItems.values().forEach(m ->
        {
            m.setParent(this);
        });

        totalBytesProcessed = new AtomicLong();
        totalBytesToXmlDocConversionDuration = new AtomicLong();
        totalXmlsProcessed = new AtomicLong();
    }

    public Path getInputPath()
    {
        return inputPath;
    }

    public Path getOriginatingFiles()
    {
        return originatingFile;
    }

    public String getName()
    {
        return name;
    }

    public Map<String, FlattenerListItem> getSpecListItems()
    {
        return Collections.unmodifiableMap(specListItems);
    }

    public void addToBytesProcessed(long bytesProcessed)
    {
        totalBytesProcessed.addAndGet(bytesProcessed);
    }

    public void addXmlsProcessed()
    {
        totalXmlsProcessed.incrementAndGet();
    }

    public void addToXmlDocConversionDuration(long durationNanos)
    {
        totalBytesToXmlDocConversionDuration.addAndGet(durationNanos);
    }

    public long getXmlsProcessed()
    {
        return totalXmlsProcessed.get();
    }

    public long getTotalBytesProcessed()
    {
        return totalBytesProcessed.get();
    }

    public long getTotalXmlConversionDuration()
    {
        return totalXmlConversionDuration.get();
    }
}

enum XmlFlattenerSourceType
{
    xpath,
    eval,
    dynAttribute,
    dynElement
}

class FlattenerListItem implements Closeable
{
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(FlattenerListItem.class);
    private static final Boolean XmlFlattener_EnableParallelJavaLambdaStreams = XMLFlattener_PropertyManager.XmlFlattener_EnableParallelJavaLambdaStreams;
    private volatile XmlFlattenerSpec parent; //this is set later when the parent is added

    //Set up Lazily
    private volatile List<String> columnNames;
    private volatile List<XmlFlattenerSpecColumn> allColumns;
    private volatile CSVPrinter csvPrinter;
    private volatile List<LayerRowsContainer> containers;
    private volatile Boolean matchesExistingTable;
    private volatile String matchingDatabaseTable;
    private volatile Path outputCSVFile;

    //Preprossing (if required)
    private volatile List<LayerRowsContainer> preprocess_containers;

    private final String mapName;
    private final String outputPath;
    private final List<XmlFlattenerSpecColumn> columns;
    private final List<XmlFlattenerExplodeItem> explodeItems;
    private final AtomicLong xmlsProcessed;
    private final AtomicLong csvRowsWritten;
    private final AtomicLong processDocumentDurations;

    public static FlattenerListItem create(final String mapName,
                                           final String outputPath)
    {
        return new FlattenerListItem(mapName, outputPath);
    }

    private FlattenerListItem(String mapName, String outputPath)
    {
        this.mapName = mapName;
        this.outputPath = outputPath;

        columns = new ArrayList<>();
        explodeItems = new ArrayList<>();
        xmlsProcessed = new AtomicLong();
        csvRowsWritten = new AtomicLong();
        processDocumentDurations = new AtomicLong();
    }

    public void preProcessRow(ParameterBag paramBag)
    {
        xmlsProcessed.incrementAndGet();
        long startOfProcessRow = System.nanoTime();
        preProcessRow(paramBag, preprocess_containers);
        processDocumentDurations.addAndGet(System.nanoTime() - startOfProcessRow);
    }

    public void processRow(ParameterBag paramBag)
    {
        if (containers == null || csvPrinter == null)
        {
            throw new RuntimeException("You cannot process a row as you have not successfully run : setUpCSVPrinterAndContainers() for - " + getMapName());
        }
        xmlsProcessed.incrementAndGet();
        long startOfProcessRow = System.nanoTime();
        processRow(paramBag, allColumns, containers, csvPrinter, csvRowsWritten);
        processDocumentDurations.addAndGet(System.nanoTime() - startOfProcessRow);
    }

    /**
     * Checks whether this item has dynamic columns.
     * If it does, then you need to resolve these first with pre-processing.
     * @return
     */
    public boolean containsDynamicColunns()
    {
        Optional<XmlFlattenerSpecColumn> items = columns.stream().filter(col -> col.getType().equals(XmlFlattenerSourceType.dynAttribute)).findAny();
        return items.isPresent();
    }

    public long getXmlsProcessed()
    {
        return xmlsProcessed.get();
    }

    public long getCsvRowsWritten()
    {
        return csvRowsWritten.get();
    }

    public Boolean getMatchesExistingTable()
    {
        if (matchesExistingTable == null) return false;
        return matchesExistingTable.booleanValue();
    }

    public long getProcessDocumentDurations()
    {
        return processDocumentDurations.get();
    }

    public Path getOutputCSVFile()
    {
        return outputCSVFile;
    }

    static void preProcessRow( ParameterBag paramBag,
                               List<LayerRowsContainer> containers)
    {
        Objects.requireNonNull(containers, "Containers must have something in there ! - ");

        final Stream<LayerRowsContainer> lrContainers = XmlFlattener_EnableParallelJavaLambdaStreams ? containers.parallelStream() : containers.stream();
        lrContainers.forEach(c -> c.preProcessDocument(paramBag));
    }

    static int processRow(ParameterBag paramBag,
                          List<XmlFlattenerSpecColumn> allColumns,
                          List<LayerRowsContainer> containers,
                          CSVPrinter csvPrinterParam,
                          AtomicLong csvRowsWritten)
    {
        for(LayerRowsContainer c : containers)
        {
            c.clear();
            c.processDocument(paramBag);
        }

        try
        {
            final List<LayerRow> rows = LayerRowsContainer.mergeLayerRows(containers);
            int rowsWritten = LayerRow.writeToCSV(allColumns, rows, csvPrinterParam);
            csvRowsWritten.addAndGet(rowsWritten);
            return rowsWritten;
        } catch (IOException e)
        {
            String msg = "Encountered an IO Exception attempting to Write to the file: " + e;
            logger.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    void setMatchesExistingTable(boolean p) throws IOException
    {
        if (matchesExistingTable != null)
        {
            throw new RuntimeException("You have already set u matchesExistingTable " + getMapName() + " - you cannot do so again!");
        }
        matchesExistingTable = Boolean.valueOf(p);
    }

    synchronized void setUpContainersForPreProcssing() throws IOException
    {
        preprocess_containers = XmlFlattener.createLevelRowContainers(this, false);
    }

    synchronized void setUpCSVPrinterAndContainers(Path rootPath) throws IOException
    {
        if (containers != null || csvPrinter != null)
        {
            throw new RuntimeException("You have already set up the containers and CSV file for " + getMapName() + " - you cannot do so again!");
        }

        outputCSVFile = rootPath.resolve(getOutputPath());
        matchingDatabaseTable = outputCSVFile.getFileName().toString().replace(".csv", "");
        final Path csvPattern = outputCSVFile.getParent();
        try
        {
            Files.createDirectories(csvPattern);
        } catch (IOException ex)
        {
            throw new RuntimeException("Create a new directory : " + csvPattern.toAbsolutePath());
        }

        containers = XmlFlattener.createLevelRowContainers(this, true);
        columnNames = LayerRowsContainer.getColumnNames(containers);
        allColumns = LayerRowsContainer.getColumns(containers);
        csvPrinter = new CSVPrinter(Files.newBufferedWriter(outputCSVFile), CSVFormat.EXCEL);
        writeCSVHeaders(csvPrinter, columnNames);
    }

    private void writeCSVHeaders(CSVPrinter csvPrinter, List<String> columnNames) throws IOException
    {
        csvPrinter.printRecord(columnNames);
    }

    void setParent(XmlFlattenerSpec parent)
    {
        this.parent = parent;
    }

    public XmlFlattenerSpec getParent()
    {
        return parent;
    }

    public String getMapName()
    {
        return mapName;
    }

    public String getOutputPath()
    {
        return outputPath;
    }

    public String getMatchingDatabaseTable()
    {
        return matchingDatabaseTable;
    }

    public List<XmlFlattenerSpecColumn> getColumns()
    {
        return columns;
    }

    public List<XmlFlattenerExplodeItem> getExplodeItems()
    {
        return explodeItems;
    }

    @Override
    public void close() throws IOException
    {
        if (csvPrinter != null) csvPrinter.close(true);
    }
}

class XmlFlattenerExplodeItem
{
    private final Optional<XmlFlattenerExplodeItem> parent;
    private final List<XmlFlattenerSpecColumn> allColumns;
    private final List<XmlFlattenerExplodeItem> allExplodeItems;

    private final int level;

    private final String name;
    private final String source;
    private final XPathExpression xPathExpression;
    private final OptionalInt circuitBreaker;

    public static XmlFlattenerExplodeItem create(int level,
                                                 Optional<XmlFlattenerExplodeItem> parent,
                                                 String name,
                                                 String source,
                                                 OptionalInt circuitBreaker)
    {
    return new XmlFlattenerExplodeItem(level, parent, name, source, circuitBreaker);
    }

    private XmlFlattenerExplodeItem(int level,
                                    Optional<XmlFlattenerExplodeItem> parent,
                                    String name,
                                    String source,
                                    OptionalInt circuitBreaker)
    {
        this.level = level;
        this.parent = parent;
        this.name = name;
        this.source = source;
        this.circuitBreaker = circuitBreaker;

        try
        {
            xPathExpression = XmlFlattenerSpec.xPath.compile(source);
        }
        catch (XPathExpressionException e)
        {
            throw new RuntimeException("The XPath specified for the explodeitem - " + name + " is not valid.  Your expression was: " + source + " - error was: " + e, e);
        }

        allColumns = new ArrayList<>();
        allExplodeItems = new ArrayList<>();
    }

    public Optional<XmlFlattenerExplodeItem> getParent()
    {
        return parent;
    }

    public int getLevel()
    {
        return level;
    }

    public String getName()
    {
        return name;
    }

    public OptionalInt getCircuitBreaker()
    {
        return this.circuitBreaker;
    }

    public String getSource()
    {
        return source;
    }

    public XPathExpression getXpathExpression()
    {
        return xPathExpression;
    }

    public List<XmlFlattenerSpecColumn> getAllColumns()
    {
        return allColumns;
    }

    public List<XmlFlattenerExplodeItem> getAllExplodeItems()
    {
        return allExplodeItems;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        XmlFlattenerExplodeItem that = (XmlFlattenerExplodeItem) o;
        return level == that.level && Objects.equals(name, that.name) && Objects.equals(source, that.source);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(level, name, source);
    }

    @Override public String toString()
    {
        return "XmlFlattenerExplodeItem{" + "level=" + level + ", name='" + name + '\'' + ", source='" + source + '\'' + ", xPathExpression=" + xPathExpression + ", circuitBreaker=" + circuitBreaker + '}';
    }
}

class XmlFlattenerSpecColumn implements Comparable<XmlFlattenerSpecColumn>
{
    private static final org.apache.logging.log4j.Logger s_log = LogManager.getLogger(XmlFlattenerSpecColumn.class);

    private final Optional<XmlFlattenerExplodeItem> parent;
    private final String columnName;
    private final XmlFlattenerSourceType type;
    private final String source;
    private final int overallOrder;
    private final int level;

    private final Optional<String> attributeFilter;
    private final Optional<XPathExpression> xpathExpression;
    private final Optional<Pattern> regexOfAttributes;

    private final Map<String , XmlFlattenerSpecColumn> resolvedColumns;

    public void addResolvedColumns(Element element)
    {
        if(regexOfAttributes.isPresent() == false)
        {
            throw new IllegalArgumentException("You cannot add Resolved Columns - as this column type is not dynamic");
        }

        final NamedNodeMap allAttributes = element.getAttributes();
        int length = allAttributes.getLength();
        for (int i = 0; i < length; i++)
        {
            Attr attr = (Attr)allAttributes.item(i);
            String name = attr.getName();
            if(resolvedColumns.containsKey(name))
            {
                continue;
            }

            if(regexOfAttributes.get().matcher(name).matches() == true)
            {
                resolvedColumns.put(name, createResolvedClone(name));
            }
        }
    }

    public Collection<XmlFlattenerSpecColumn> getResolvedColumns()
    {
        return resolvedColumns.values();
    }

    private XmlFlattenerSpecColumn createResolvedClone(String name)
    {
        try
        {
            return new XmlFlattenerSpecColumn(level,
                                              overallOrder,
                                              parent,
                                              columnName + "_" + name,
                                              XmlFlattenerSourceType.xpath,
                                              source.trim().equals("") ? source + "@" + name : source + "/@" + name,
                                              attributeFilter);
        }
        catch (XPathExpressionException e)
        {
            throw new RuntimeException("Unable to create expanded XPath for Attribute : " + name + " - " + e, e);
        }
    }

    public static XmlFlattenerSpecColumn create(int level,
                                                int overallOrder,
                                                Optional<XmlFlattenerExplodeItem> parent,
                                                String columnName,
                                                XmlFlattenerSourceType type,
                                                String source,
                                                Optional<String> attributeFilter) throws XPathExpressionException
    {
        return new XmlFlattenerSpecColumn(level, overallOrder, parent, columnName, type, source, attributeFilter);
    }

    private XmlFlattenerSpecColumn(int level,
                                   int overallOrder,
                                   Optional<XmlFlattenerExplodeItem> parent,
                                   String columnName,
                                   XmlFlattenerSourceType type,
                                   String source,
                                   Optional<String> attributeFilterP) throws XPathExpressionException
    {
        this.parent = parent;
        this.level = level;
        this.overallOrder = overallOrder;
        this.columnName = columnName;
        this.type = type;
        this.source = source;
        this.resolvedColumns = new ConcurrentHashMap<>();
        this.attributeFilter = attributeFilterP;

        if (type == XmlFlattenerSourceType.xpath)
        {
            regexOfAttributes = Optional.empty();
            try
            {
                xpathExpression = Optional.of(XmlFlattenerSpec.xPath.compile(source));
            }
            catch (XPathExpressionException e)
            {
                throw new RuntimeException("The Xpath specified for column - " + columnName + " - is not valid.  Your expression was - " + source + " - error was : " + e, e);
            }
        }
        else if(type == XmlFlattenerSourceType.dynAttribute)
        {
            regexOfAttributes = Optional.of(Pattern.compile(attributeFilter.get()));
            try
            {
                xpathExpression = Optional.of(XmlFlattenerSpec.xPath.compile(source));
            }
            catch (XPathExpressionException e)
            {
                throw new RuntimeException("The Xpath specified for column [" + type + "] - " + columnName + " - is not valid.  Your expression was - " + source + " - error was : " + e, e);
            }
        }
        else
        {
            xpathExpression = Optional.empty();
            regexOfAttributes = Optional.empty();
        }
    }

    public String getColumnName()
    {
        return columnName;
    }

    public XmlFlattenerSourceType getType()
    {
        return type;
    }

    public String getSource()
    {
        return source;
    }

    @Override
    public String toString()
    {
        return "uk.co.devworx.xmlflattener.XmlExtractorSpecColumn{" + "columnName='" + columnName + '\'' + ", type=" + type + ", source='" + source + '\'' + '}';
    }

    public Optional<XmlFlattenerExplodeItem> getParent()
    {
        return parent;
    }

    public int getOverallOrder()
    {
        return getOverallOrder();
    }

    public int getLevel()
    {
        return getLevel();
    }

    public Optional<XPathExpression> getXpathExpression()
    {
        return xpathExpression;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        XmlFlattenerSpecColumn that = (XmlFlattenerSpecColumn) o;
        return Objects.equals(columnName, that.columnName) && type == that.type && Objects.equals(source, that.source);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, type, source);
    }

    @Override
    public int compareTo(XmlFlattenerSpecColumn o)
    {
        if (overallOrder > o.overallOrder)
        {
            return 1;
        }
        if (overallOrder < o.overallOrder)
        {
            return -1;
        }
        return columnName.compareTo(o.columnName);
    }

    public List<NodeList> getRecursiveParentNodeList(final ParameterBag parameterBag)
    {
        final List<NodeList> fullList = new ArrayList<>();
        final List<XmlFlattenerExplodeItem> lineage = new ArrayList<>();
        Optional<XmlFlattenerExplodeItem> parentItem = getParent();
        if (parentItem.isPresent() == false)
        {
            throw new IllegalArgumentException("You cannot get the recursive parent node list for a column that does not have a parent!");
        }
        do
        {
            XmlFlattenerExplodeItem p = parentItem.get();
            lineage.add(p);
            parentItem = p.getParent();
        }
        while (parentItem.isPresent() == true);

        final XmlFlattenerExplodeItem deepestExplodeItem = lineage.get(0);
        Collections.reverse(lineage); //working top down

        final XmlFlattenerExplodeItem topExplodeItem = lineage.get(0);
        final int index = 1;

        try
        {
            final NodeList ndeList = ___recurseNodeListDown_evaluate(topExplodeItem,  topExplodeItem.getXpathExpression(),  parameterBag.getThreadSafeXmlDoc());
            recurseNodeListDown(index, lineage, ndeList, fullList);
        } catch (XPathExpressionException xe)
        {
            throw new RuntimeException("Unable to parse the xpath against root nodeset for : " + topExplodeItem + " | " + xe, xe);
        }
        return fullList;
    }

    private List<Element> ___direct_node_resolve(final Node subject, final String elementMatchingName, final XmlFlattenerExplodeItem explodeItem)
    {
        final List<Element> resultNodes = new ArrayList<>();
        if (subject instanceof Element == false && subject instanceof Document == false) return resultNodes;

        NodeList childNodes = null;
        if (subject instanceof Document)
        {
            Document doc = (Document) subject;
            childNodes = doc.getChildNodes();
        }
        if (subject instanceof Element)
        {
            Element el = (Element) subject;
            childNodes = el.getChildNodes();
        }
        final OptionalInt circuitBreaker = explodeItem.getCircuitBreaker();

        for (int i = 0; i < childNodes.getLength(); i++)
        {
            if (circuitBreaker.isPresent() == true && i >= circuitBreaker.getAsInt())
            {
                s_log.warn("Invoking the Circuit Breaker for " + explodeItem.getName() + " - Total Item Count : " + i + ".  You may need to review your Exploded Items and the Expression : " + explodeItem.getSource() + ". Try flattening the levels it goes through instead.");
                break;
            }
            final Node item = childNodes.item(i);
            if (item instanceof Element)
            {
                Element e = (Element) item;
                final String nodeName = e.getNodeName();
                final String localName = nodeName.indexOf(":") != -1 ? nodeName.substring(nodeName.indexOf(":") + 1) : nodeName;
                if (localName.equals(elementMatchingName) || nodeName.equals(elementMatchingName))
                {
                    resultNodes.add(e);
                }
            }
        }
        return resultNodes;
    }

    /**
     * See if we can use the single-level resolve for this item - effectively optimising the XPath
     * resolution path;
     *
     * @param source
     * @return
     */
    private boolean ___isSingleLevelResolve(String source)
    {
        return (source.indexOf('/') == -1 && source.contains("[") == false && source.contains("]") == false);
    }

    /**
     * See if we can use the two-level resolve for this item - effectively optimising the XPath
     * resolution path;
     *
     * @param source
     * @return
     */
    private boolean ___isTwoLevelResolve(String source)
    {
        int index = source.indexOf('/');
        if (index == -1) return false;
        if (index == source.length() - 1) return false;

        String val1 = source.substring(0, index);
        String val2 = source.substring(index + 1, source.length());

        return ___isSingleLevelResolve(val1) && ___isSingleLevelResolve(val2);
    }

    private NodeList ___recurseNodeListDown_evaluate_single_level_resolve(final XmlFlattenerExplodeItem subjectExplodeItem,
                                                                          final XPathExpression xpathExpression,
                                                                          final Node node) throws XPathExpressionException
    {
        final String source = subjectExplodeItem.getSource();
        final List<Element> items = ___direct_node_resolve(node, source, subjectExplodeItem);
        return new NodeListBasedOnList(items);
    }

    private NodeList ___recurseNodeListDown_evaluate_two_level_resolve(final XmlFlattenerExplodeItem subjectExplodeItem,
                                                                       final XPathExpression xpathExpression,
                                                                       final Node node) throws XPathExpressionException

    {
        final String source = subjectExplodeItem.getSource();
        int index = source.indexOf('/');
        String val1 = source.substring(0, index);
        String val2 = source.substring(index + 1, source.length());

        final List<Element> overall = new ArrayList<>();
        final List<Element> items = ___direct_node_resolve(node, val1, subjectExplodeItem);
        for (Element e : items)
        {
            final List<Element> items2 = ___direct_node_resolve(e, val2, subjectExplodeItem);
            overall.addAll(items2);
        }
        return new NodeListBasedOnList(overall);
    }

    private NodeList ___recurseNodeListDown_evaluate(final XmlFlattenerExplodeItem subjectExplodeItem,
                                                     final XPathExpression xpathExpression,
                                                     final Node node) throws XPathExpressionException

    {
        if (___isSingleLevelResolve(subjectExplodeItem.getSource()) == true)
        {
            return ___recurseNodeListDown_evaluate_single_level_resolve(subjectExplodeItem, xpathExpression, node);
        }
        if (___isTwoLevelResolve(subjectExplodeItem.getSource()) == true)
        {
            return ___recurseNodeListDown_evaluate_two_level_resolve(subjectExplodeItem, xpathExpression, node);
        }
        try
        {
            NodeList nodeList = (NodeList) xpathExpression.evaluate(node, XPathConstants.NODESET);
            return nodeList;
        } catch (OutOfMemoryError oome)
        {
            String msg = "Encountered and OOME while attempting to evaluate the XPath  "
                    + "[expression=" + subjectExplodeItem.getSource() + "][subjectExplodeItem_Name=" + subjectExplodeItem.getName() + ",level=" + subjectExplodeItem.getLevel() + "]- Aborted.  " + oome
                    + "\n There is not much we can do.  Please try another XPath.";
            s_log.error(msg, oome);
            throw oome;
        }
    }

    private void recurseNodeListDown(final int indexOfList,
                                     final List<XmlFlattenerExplodeItem> explodeItems,
                                     final NodeList currentNodeList,
                                     final List<NodeList> resultNodeList)
    {
        if (indexOfList >= explodeItems.size())
        {
            resultNodeList.add(currentNodeList);
            return;
        }
        final XmlFlattenerExplodeItem subjectExplodeItem = explodeItems.get(indexOfList);

        for (int i=0; i < currentNodeList.getLength(); i++)
        {
            Node node = currentNodeList.item(i);

            try
            {
                final XPathExpression xpathExpression = subjectExplodeItem.getXpathExpression();
                final NodeList nodeList = ___recurseNodeListDown_evaluate(subjectExplodeItem, xpathExpression, node);
                {
                    recurseNodeListDown(indexOfList + 1, explodeItems, nodeList, resultNodeList);
                }
            } catch (XPathExpressionException xe)
            {
                throw new RuntimeException("Unable to parse XPath against nodeset for : " + subjectExplodeItem + " | " + xe, xe);
            }
        }
    }
}

class NodeListBasedOnList implements NodeList
{
    private final List<Element> elements;

    public NodeListBasedOnList(List<Element> elementsP)
    {
        elements = elementsP;
    }

    @Override
    public Node item(int index)
    {
        return elements.get(index);
    }

    @Override
    public int getLength()
    {
        return elements.size();
    }

}
