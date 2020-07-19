package uk.co.devworx.xmlflattener;

import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A simple wrapper for representing a row object.
 */
public class LayerRow
{
    private static final Logger logger = LogManager.getLogger(LayerRow.class);
    private final List<XmlFlattenerSpecColumn> rowColumns;
    private final List<String> rowItems;

    static LayerRow create(List<String> rowItems, List<XmlFlattenerSpecColumn> columns ){
        return new LayerRow(Collections.unmodifiableList(rowItems), Collections.unmodifiableList(columns));
    }

    private LayerRow(List<String> rowItems, List<XmlFlattenerSpecColumn> columns ){
        this.rowItems = rowItems;
        this.rowColumns = columns;
        if(rowItems.size() != rowColumns.size())
        {
            String errMsg = "Mismatch between row items and columns - " + rowItems.size() + " vs. " + rowColumns.size() + "\n" +
                    columns.stream().map(c->c.getColumnName()).collect(Collectors.toList()) +
                    rowItems.toString() + "\n";
            throw new IllegalArgumentException(errMsg);
        }
    }

    /**
     * Creates the derived rows for the new value.
     * @param val
     * @return
     */
    public static List<LayerRow> derivedRows(List<LayerRow> rows, String val, XmlFlattenerSpecColumn col)
    {
        final List<LayerRow> derivedRows = new ArrayList<>();
        for(LayerRow row : rows)
        {
            List<String> newList = new ArrayList<>(row.rowItems);
            List<XmlFlattenerSpecColumn> newColList = new ArrayList<>(row.rowColumns);

            newList.add(0, val.trim());
            newColList.add(0, col);

            logger.debug("val : + " + val);
            logger.debug("col : + " + col.getColumnName());
            logger.debug("newList : + " + newList);

            derivedRows.add(new LayerRow(newList, newColList));

        }
        return derivedRows;
    }

    public static List<LayerRow> derivedRows(List<LayerRow> rows, LayerRow val){
        final List<LayerRow> derivedRows = new ArrayList<>();
        for(LayerRow row : rows){
            List<String> newList = new ArrayList<>(row.rowItems.size() + val.rowItems.size());
            List<XmlFlattenerSpecColumn> newColList = new ArrayList<>(row.rowColumns.size() + val.rowColumns.size());

            newList.addAll(val.rowItems);
            newList.addAll(row.rowItems);
            newColList.addAll(val.rowColumns);
            newColList.addAll(row.rowColumns);

            /*
            List<String> newList = new ArrayList<>(row.rowItems);
            List<XmlExtractorSpecColumn> newColList = new ArrayList<>(row.rowColumns);

            newList.addAll(0, val.rowItems);
            newColList.addAll(0, val.rowColumns);
             */

            derivedRows.add(new LayerRow(newList, newColList));
        }
        return derivedRows;
    }

    /**
     * Create a row object for the items where there are no columns deeper in the structure
     *
     * @param val
     * @return
     */
    public static LayerRow createRootRow(String val, XmlFlattenerSpecColumn col)
    {
        LayerRow row = new LayerRow(Collections.singletonList(val.trim()), Collections.singletonList(col));
        return row;
    }

    public static int writeToCSV(List<XmlFlattenerSpecColumn> columnOrder, List<LayerRow> layerRows, CSVPrinter csvPrinter) throws IOException
    {
        int count= 0;
        for(LayerRow r : layerRows){
            List<String> row = new ArrayList<>();
            for(XmlFlattenerSpecColumn col : columnOrder){
                int index = r.rowColumns.indexOf(col);
                String val = (index == -1) ? "" : r.rowItems.get(index);
                row.add(val);
            }
            count++;
            csvPrinter.printRecord(row);
        }
        return count;
    }

    public static String toString(List<LayerRow> layerRows){
        final SortedSet<XmlFlattenerSpecColumn> ss = new TreeSet<>();
        layerRows.forEach(r-> ss.addAll(r.rowColumns));

        final AsciiTable at = new AsciiTable();

        at.addRule();
        at.addRow(ss.stream().map(s -> s.getColumnName()).collect(Collectors.toList()));
        at.addRule();

        for(LayerRow r : layerRows){
            List<String> row = new ArrayList<>();
            for(XmlFlattenerSpecColumn col : ss){
                int index = r.rowColumns.indexOf(col);
                String val = (index == -1) ? "" : r.rowItems.get(index);
                row.add(val);
            }
            at.addRow(row);
        }
        at.addRule();

        CWC_LongestLine cwc = new CWC_LongestLine();
        at.getRenderer().setCWC(cwc);
        return  "\n" + at.render();
    }

    @Override public String toString()
    {
        final TreeMap<XmlFlattenerSpecColumn, String> sortedItems = new TreeMap<>();
        for(int i = 0; i < rowColumns.size(); i++)
        {
            sortedItems.put(this.rowColumns.get(i), rowItems.get(i));
        }
        final AsciiTable at = new AsciiTable();

        at.addRule();
        at.addRow(sortedItems.keySet().stream().map(s -> s.getColumnName()).collect(Collectors.toList()));
        at.addRule();
        at.addRow(sortedItems.values());
        at.addRule();

        CWC_LongestLine cwc = new CWC_LongestLine();
        at.getRenderer().setCWC(cwc);
        return "\n" + at.render();
    }

}
