package uk.co.devworx.xmlflattener;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class handles all the values at a specific level of the XML flattening hierarchy
 */
public class LayerRowsContainer
{
    private static final Logger logger = Logger.getLogger(LayerRowsContainer.class);
    private static final boolean XmlFlattener_EnableParallelJavaLambdaStreams = XMLFlattener_PropertyManager.XmlFlattener_EnableParallelJavaLambdaStreams;

    private final int layer;
    private final List<XmlFlattenerSpecColumn> xmlFlattenerColumns;
    private final ConcurrentMap<XmlFlattenerSpecColumn, List<String>> columnValues;
    private final List<LayerRow> layerRows;
    private final String name;

    public static List<LayerRow> mergeLayerRows(LayerRowsContainer... containerParams)
    {
        return mergeLayerRows(Arrays.asList(containerParams));
    }

    public static List<LayerRow> mergeLayerRows(Collection<LayerRowsContainer> containerParams)
    {
        final List<LayerRowsContainer> containers = new ArrayList<>();
        Comparator<LayerRowsContainer> comp = (o1, o2) -> {
            if(o1.getLayer() > o2.getLayer()){
                return -1; // we want inverse order
            }
            if(o1.getLayer() < o2.getLayer()){
                return 1;
            }
            return 0;
        };

        containers.addAll(containerParams);
        Collections.sort(containers, comp);

        List<LayerRow> inner_layerRows = new ArrayList<>();

        inner_layerRows.addAll(containers.get(0).getLayerRows());

        for(int i = 1; i < containers.size(); i++){
            final List<LayerRow> nextGenLayerRows = new ArrayList<>();
            final LayerRowsContainer c = containers.get(i);
            for(LayerRow v : c.getLayerRows()){
                List<LayerRow> layerRows = LayerRow.derivedRows(inner_layerRows, v);
                nextGenLayerRows.addAll(layerRows);
            }
            inner_layerRows = nextGenLayerRows;
        }
        return Collections.unmodifiableList(inner_layerRows);
    }

    public static LayerRowsContainer create(int level, String name, List<XmlFlattenerSpecColumn> xmlExtractColumns, boolean expandResolvedItems)
    {
        return new LayerRowsContainer(level, name, xmlExtractColumns, expandResolvedItems);
    }

    private LayerRowsContainer(int layer,
                               String name,
                               List<XmlFlattenerSpecColumn> xmlFlattenerColumnsP,
                               boolean expandResolvedItems)
    {
        if(xmlFlattenerColumnsP == null || xmlFlattenerColumnsP.isEmpty() == true)
        {
            String report = "You are required to have at least 1 column per 'level' in your XML-Flattener definition. It would appear that the '" + name + "' definition does not have any (non exploded) columns";
            throw new IllegalArgumentException(report);
        }

        this.layer = layer;
        this.name = name;

        if(expandResolvedItems == false)
        {
            this.xmlFlattenerColumns = Collections.unmodifiableList(new CopyOnWriteArrayList<>(xmlFlattenerColumnsP));
        }
        else
        {
            List<XmlFlattenerSpecColumn> preCols = new CopyOnWriteArrayList<>();
            for(XmlFlattenerSpecColumn c : xmlFlattenerColumnsP)
            {
                if(c.getType().equals(XmlFlattenerSourceType.dynAttribute) == true)
                {
                    preCols.addAll(c.getResolvedColumns());
                }
                else
                {
                    preCols.add(c);
                }
            }
            this.xmlFlattenerColumns = Collections.unmodifiableList(preCols);
        }

        this.columnValues = new ConcurrentSkipListMap<>();
        this.layerRows = new CopyOnWriteArrayList<>();
        clear();
    }

    public static List<XmlFlattenerSpecColumn> getColumns(List<LayerRowsContainer> containers)
    {
        final TreeSet<XmlFlattenerSpecColumn> allCols = new TreeSet<>();
        for(LayerRowsContainer c :containers){
            allCols.addAll(c.getXmlExtractorColumns());
        }
        return Collections.unmodifiableList(new ArrayList<>(allCols));
    }
    public static List<String> getColumnNames(List<LayerRowsContainer> containers)
    {
        return getColumns(containers).stream().map(c-> c.getColumnName()).collect(Collectors.toList());
    }

    public List<XmlFlattenerSpecColumn> getXmlExtractorColumns()
    {
        return xmlFlattenerColumns;
    }
    public List<LayerRow> getLayerRows()
    {
        return Collections.unmodifiableList(layerRows);
    }

    public int getLayer()
    {
        return layer;
    }
    public String getName(){return name;}

    public void clear()
    {
        columnValues.clear();
        for(XmlFlattenerSpecColumn col : xmlFlattenerColumns)
        {
            columnValues.put(col, new ArrayList<>());
        }
        layerRows.clear();
    }

    /**
     * Process the document, put it in the internal structure and normalise the rows
     * @param paramBag
     */
    public void processDocument(ParameterBag paramBag)
    {
        if(xmlFlattenerColumns == null || xmlFlattenerColumns.isEmpty() == true)
        {
            throw new IllegalArgumentException("Layer - " + layer + " - Name - " + name + " - processDocument() -- Has Column Length of " + xmlFlattenerColumns.size());
        }

        //Resolve the values
        final Stream<XmlFlattenerSpecColumn> specStream = XmlFlattener_EnableParallelJavaLambdaStreams ? xmlFlattenerColumns.parallelStream() : xmlFlattenerColumns.stream();
        specStream.forEach(col -> ___processDocumentColumn(col, paramBag));

        //Now normalise the level rows
        final SortedMap<XmlFlattenerSpecColumn, List<String>> depthFirst = new TreeMap<>((o1, o2) ->
        {
            int len1 = columnValues.get(o1).size();
            int len2 = columnValues.get(o2).size();
            if(len1 > len2 ) return -1;
            if(len1 < len2 ) return 1;
            return o1.getColumnName().compareTo(o2.getColumnName());

        });
        depthFirst.putAll(columnValues);

        if(columnValues.isEmpty() == true)
        {
            throw new RuntimeException("Unexpected case - " + getName() + " @ LEVEL " + getLayer() + "Has no columns ?");
        }
        if(logger.isDebugEnabled())
        {
            logger.debug("Sorted Column Values : " + depthFirst.keySet().stream().map(c -> c.getColumnName()).collect(Collectors.toList()));
        }

        final List<String> lastItem = depthFirst.get(depthFirst.lastKey());
        if(logger.isDebugEnabled()){
            List<String> firstItem  = depthFirst.get(depthFirst.lastKey());
            logger.debug("Level is : " + getLayer() + " | Deepest Item : " + lastItem.size() + " | Shallowest Item : " + firstItem.size());
        }

        final List<XmlFlattenerSpecColumn> newRowColumns = new ArrayList<>();
        newRowColumns.addAll(depthFirst.keySet());
        List<LayerRow> result_layerRows = new ArrayList<>();
        int currentIndex = 0;

        for(int i = 0; i < lastItem.size() ; i++){
            List<String> rowItems = new ArrayList<>(depthFirst.size());
            for(Map.Entry<XmlFlattenerSpecColumn, List<String>> e : depthFirst.entrySet()){
                List<String> items = e.getValue();
                if(i >= items.size()){
                    rowItems.add("");
                }
                else{
                    rowItems.add(items.get(i));
                }
            }
            result_layerRows.add(LayerRow.create(rowItems, newRowColumns));
        }
        layerRows.addAll(result_layerRows);
    }

    /**
     * Process the document, put it in the internal structure and normalise the rows
     * @param paramBag
     */
    public void preProcessDocument(ParameterBag paramBag){
        //Resolve the values
        final Stream<XmlFlattenerSpecColumn> specStream = XmlFlattener_EnableParallelJavaLambdaStreams ? xmlFlattenerColumns.parallelStream() : xmlFlattenerColumns.stream();
        specStream.forEach(col -> ___preProcessDocumentColumn(col, paramBag));
    }

    private void ___preProcessDocumentColumn(XmlFlattenerSpecColumn col, ParameterBag paramBag)
    {
        if(col.getType().equals(XmlFlattenerSourceType.dynAttribute) == false) return;

        Optional<XPathExpression> xpathOpt = col.getXpathExpression();
        if(xpathOpt.isPresent() == false)
        {
            return;
        }

        final XPathExpression xpath = xpathOpt.get();
        if(col.getParent().isPresent() == false)
        {
            ___preProcessDocumentColumn_evaluate_xpath(col, paramBag, xpath);
        }
        else
        {
            ___preProcessDocumentColumn_evaluate_xpath_parent_recurse(col, paramBag, xpath);
        }

    }

    private void ___processDocumentColumn(XmlFlattenerSpecColumn col, ParameterBag paramBag)
    {
        Optional<XPathExpression> xpathOpt = col.getXpathExpression();
        if(xpathOpt.isPresent() == false)
        {
            ___processDocumentColumn_non_xpath(col, paramBag);
            return;
        }

        final XPathExpression xpath = xpathOpt.get();
        if(col.getParent().isPresent() == false)
        {
            ___processDocumentColumn_evaluate_xpath(col, paramBag, xpath);
        }
        else
        {
            ___processDocumentColumn_evaluate_xpath_parent_recurse(col, paramBag, xpath);
        }

        if(logger.isDebugEnabled() == true)
        {
            StringBuilder bldr = new StringBuilder();

            columnValues.get(col).forEach(s ->
            {
                bldr.append("'" + s + "'");
                bldr.append(",");
            });

            bldr.deleteCharAt(bldr.length() - 1);
            logger.info("At the end of the doc process - " + col.getColumnName() + " (Layer: " + getLayer() + ")- Has total of " + columnValues.get(col).size() + " : " + bldr );
        }
    }

    private void ___processDocumentColumn_non_xpath(XmlFlattenerSpecColumn col, ParameterBag paramBag){
        switch(col.getType())
        {

            case xpath: throw new IllegalStateException("This can't happen as it should've been caught by XPath optional!");

            case eval:
                final String sourceVal = col.getSource();
                if(sourceVal.equalsIgnoreCase(ParameterBag.BATCH_TIME_COLNAME) == true ||
                   sourceVal.equalsIgnoreCase(ParameterBag.CURRENT_TIME_COLNAME) == true)
                {
                    columnValues.get(col).add(paramBag.getBatchtime().toString());
                }
                else
                {
                    columnValues.get(col).add(sourceVal);
                }
                return;

            case dynAttribute:
                throw new IllegalArgumentException("At this point in the non-xml parsing - " + col.getType() + " type. Should already have been resolved. Something wrong with the coding logic?");

            default:
                throw new IllegalArgumentException("No idea how to handle the " + col.getType() + " type.  Check if you've added code for that new enum type.");
        }
    }

    private void ___preProcessDocumentColumn_evaluate_xpath(final XmlFlattenerSpecColumn col,
                                                         final ParameterBag paramBag,
                                                         final XPathExpression xpath){
        Objects.requireNonNull(xpath, "XPath Expression cannot be null at this point");
        //At the root of the hierarchy, so we expect a single value.
        try
        {
            final Document xmlDoc = ___processDocumentColumn_evaluate_xpath_getdoc(paramBag);

            NodeList ndeList = (NodeList)xpath.evaluate(xmlDoc, XPathConstants.NODESET);
            for (int i = 0; i < ndeList.getLength(); i++)
            {
                Node item = ndeList.item(i);
                if(item instanceof Element)
                {
                    col.addResolvedColumns((Element)item);
                }
            }
        } catch (Exception e)
        {
            throw new RuntimeException("Unable to evaluate the xpath for the column : " + col + " | " + e, e);
        }
    }

    private void ___processDocumentColumn_evaluate_xpath(final XmlFlattenerSpecColumn col,
                                                         final ParameterBag paramBag,
                                                         final XPathExpression xpath){
        Objects.requireNonNull(xpath, "XPath Expression cannot be null at this point");
        //At the root of the hierarchy, so we expect a single value.
        try
        {
            final Document xmlDoc = ___processDocumentColumn_evaluate_xpath_getdoc(paramBag);
            String value = ___processDocumentColumn_evaluate_xpath_process(col, xmlDoc, xpath);
            columnValues.get(col).add(value);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to evaluate the xpath for the column : " + col + " | " + e, e);
        }
    }

    private String ___processDocumentColumn_evaluate_xpath_process(final XmlFlattenerSpecColumn col,
                                                                   final Document xmlDoc,
                                                                   final XPathExpression xpath) throws XPathExpressionException
    {
        try{
            return String.valueOf(xpath.evaluate(xmlDoc, XPathConstants.STRING));
        }
        catch(OutOfMemoryError oome)
        {
            String msg = "Encountered an OOME while attempting to evaluate the XPath [col=" + col.getColumnName() + ", source=" + col.getSource() + ", level=" + col.getLevel()+ "]...."+ oome
                    + "\n Please tweak your XPATH.";
            logger.error(msg, oome);
            oome.printStackTrace();
            throw oome;
        }
    }

    private Document ___processDocumentColumn_evaluate_xpath_getdoc(final ParameterBag paramBag){
        return paramBag.getThreadSafeXmlDoc();
    }

    private String ___processDocumentColumn_evaluate_xpath_parent_recurse_process(final XmlFlattenerSpecColumn col,
                                                                                  final Node node,
                                                                                  final XPathExpression xpath) throws XPathExpressionException
    {
        try
        {
            return String.valueOf(xpath.evaluate(node, XPathConstants.STRING)).trim();
        }
        catch (OutOfMemoryError oome)
        {
            String msg = "Encountered an OOME while attempting to evaluate the XPath [col=" + col.getColumnName() + ", source=" + col.getSource() + ", level=" + col.getLevel()+ "]...."+ oome
                    + "\n Please tweak your XPATH.";
            logger.error(msg, oome);
            oome.printStackTrace();
            throw oome;
        }
    }

    private void ___preProcessDocumentColumn_evaluate_xpath_parent_recurse(final XmlFlattenerSpecColumn col,
                                                                           final ParameterBag paramBag,
                                                                           final XPathExpression xpath)
    {
        //Obtain the nodelist from the parent items
        List<NodeList> nodeLists = col.getRecursiveParentNodeList(paramBag);
        logger.info("Total Recursive Parent Node Lists for - " + col.getColumnName() + " - " + nodeLists.size());
        for(NodeList nl : nodeLists)
        {
            logger.info("Sub Node List Length - " + nl.getLength());

            for(int i = 0; i < nl.getLength(); i++)
            {
                Node node = nl.item(i);
                try
                {
                    if(col.getSource().trim().equals("") && node instanceof Element)
                    {
                        col.addResolvedColumns((Element)node);
                        continue;
                    }

                    final NodeList matchingElements = (NodeList)xpath.evaluate(node, XPathConstants.NODESET);
                    for (int j = 0; j < matchingElements.getLength(); j++)
                    {
                        Node item = matchingElements.item(j);
                        if(item instanceof Element)
                        {
                            col.addResolvedColumns((Element)item);
                        }
                    }
                }
                catch (XPathExpressionException e)
                {
                    throw new RuntimeException("Unable to evaluate the xpath for the column : " + col + " - recursive node list : " + i+ " | " + e, e);
                }
            }
        }
    }

    private void ___processDocumentColumn_evaluate_xpath_parent_recurse(final XmlFlattenerSpecColumn col,
                                                                        final ParameterBag paramBag,
                                                                        final XPathExpression xpath)
    {
        //Obtain the nodelist from the parent items
        boolean hasAddedAValue = false;
        List<NodeList> nodeLists = col.getRecursiveParentNodeList(paramBag);

        logger.debug("Total Recursive Parent Node Lists for - " + col.getColumnName() + " - " + nodeLists.size());

        for(NodeList nl : nodeLists)
        {
            logger.debug("Sub Node List Length - " + nl.getLength());

            for(int i = 0; i < nl.getLength(); i++)
            {
                Node node = nl.item(i);
                try
                {
                    String value = ___processDocumentColumn_evaluate_xpath_parent_recurse_process(col, node, xpath);
                    columnValues.get(col).add(value);
                    hasAddedAValue = true;
                }
                catch (XPathExpressionException e)
                {
                    throw new RuntimeException("Unable to evaluate the xpath for the column : " + col + " - recursive node list : " + i+ " | " + e, e);
                }
            }
        }

        if(hasAddedAValue == false && columnValues.get(col).size() == 0)
        {
            columnValues.get(col).add("");
        }
    }

    @Override public String toString(){
            return "uk.co.devworx.xmlflattener.LevelRowsContainer{" + "level=" + layer + ", xmlExtractColumns=" + xmlFlattenerColumns + ", columnvalues="
                    + columnValues + ", levelRows" + layerRows + '}';
    }

}
