package uk.co.devworx.xmlflattener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Given an xml file and spec, this worker produces the flattened CSV
 */
public class XmlFlattener
{
    private static final Logger logger = LogManager.getLogger(XmlFlattener.class);
    private static final boolean XmlFlattener_EnableParallelJavaLambdaStreams = XMLFlattener_PropertyManager.XmlFlattener_EnableParallelJavaLambdaStreams;

    public static void produceCSVFlattens(final Timestamp batchTime, final XmlFlattenerSpec spec)
    {
        ___produceCSVFlattens_Local(batchTime, spec);
    }

    private static List<FlattenerListItem> getItemsRequiringResolution(final XmlFlattenerSpec spec)
    {
        return spec.getSpecListItems().values().stream().filter(f -> f.containsDynamicColunns()).collect(Collectors.toList());
    }

    public static void fullyResolveDynamicColumns(final Timestamp batchTime,
                                                  final XmlFlattenerSpec spec)
    {
        final Path inputPath = spec.getInputPath();
        try
        {

            final List<FlattenerListItem> flattenerListItems = getItemsRequiringResolution(spec);
            logger.info("There are a total of " + flattenerListItems.size() + " that needs expansion.");
            if(flattenerListItems.isEmpty() == true)
            {
                return;
            }

            for(FlattenerListItem item : flattenerListItems)
            {
                item.setUpContainersForPreProcssing();
            }

            final Path XmlInputsDirectory = getRelativeOrAbsolutePath(inputPath, XMLFlattener_PropertyManager.XmlFlattener_LocalRunXMLDirectory);
            final List<Path> allXMLFiles = getAllXmlFiles(XmlInputsDirectory);

            int rows = 0;
            for(int i = 0; i < allXMLFiles.size(); i++)
            {
                rows++;
                final Map<String, String> paramBag = new HashMap<>();
                FileTime lastModifiedTime = Files.getLastModifiedTime(allXMLFiles.get(i));
                paramBag.put("currenttime", batchTime.toString());

                ParameterBag paramBagPre = null;

                final byte[] data = Files.readAllBytes(allXMLFiles.get(i));
                spec.addToBytesProcessed(data.length);
                spec.addXmlsProcessed();

                long xmlConvStart = System.nanoTime();
                try
                {
                    paramBagPre = ParameterBag.create(data, batchTime, paramBag);
                }
                catch (SAXException | IOException ex)
                {
                    handleAndLogBrokenXMLInFeed(inputPath, spec, rows, ex, "Unable to read the XML for the file for ", data);
                    continue;
                }
                if(paramBagPre == null)
                {
                    throw new RuntimeException("Found a null XML document - this is not expected");
                }
                final ParameterBag paramBag2 = paramBagPre;

                spec.addToXmlDocConversionDuration(System.nanoTime() - xmlConvStart);

                final Stream<FlattenerListItem> mapListItemStream = XmlFlattener_EnableParallelJavaLambdaStreams ? flattenerListItems.parallelStream() : flattenerListItems.stream();
                mapListItemStream.forEach(m-> ___preProcessMapItemRow(m, paramBag2));

                if(rows % XMLFlattener_PropertyManager.XmlFlattener_PrintReportSize == 0)
                {
                    logger.info("Have now pre-processed a total of " + (rows) + " rows for the extractor spec : " + spec.getName());
                }
            }
            logger.info("Now pre-processed a total of " + rows + " | Closing all items ");

        }
        catch(IOException e)
        {
            throw new RuntimeException("Encountered unexpected IO Exception - something's wrong with your file system : "+ e, e);
        }
    }

    static void ___produceCSVFlattens_Local(final Timestamp batchTime, final XmlFlattenerSpec spec)
    {
        final Path inputPath = spec.getInputPath();
        try
        {
            final Map<String, FlattenerListItem> mapListItemMap = spec.getSpecListItems();
            final Collection<FlattenerListItem> flattenerListItems = mapListItemMap.values();

            for(FlattenerListItem m : flattenerListItems){
                m.setUpCSVPrinterAndContainers(inputPath);
                m.setMatchesExistingTable(false);
            }
            final Path XmlInputsDirectory = getRelativeOrAbsolutePath(inputPath, XMLFlattener_PropertyManager.XmlFlattener_LocalRunXMLDirectory);
            final List<Path> allXMLFiles = getAllXmlFiles(XmlInputsDirectory);

            int rows = 0;
            for(int i = 0; i < allXMLFiles.size(); i++)
            {
                rows++;
                final Map<String, String> paramBag = new HashMap<>();
                FileTime lastModifiedTime = Files.getLastModifiedTime(allXMLFiles.get(i));
                paramBag.put("source_xml_date", lastModifiedTime.toInstant().toString());
                paramBag.put("input_file_name", allXMLFiles.get(i).getFileName().toString());
                paramBag.put("currenttime", batchTime.toString());

                ParameterBag paramBagPre = null;

                final byte[] data = Files.readAllBytes(allXMLFiles.get(i));
                spec.addToBytesProcessed(data.length);
                spec.addXmlsProcessed();
                long xmlConvStart = System.nanoTime();
                try{
                    paramBagPre = ParameterBag.create(data, batchTime, paramBag);
                }
                catch (SAXException | IOException ex)
                {
                    handleAndLogBrokenXMLInFeed(inputPath, spec, rows, ex, "Unable to read the XML for the file for ", data);
                    continue;
                }
                if(paramBagPre == null){
                    throw new RuntimeException("Found a null XML document - this is not expected");
                }
                final ParameterBag paramBag2 = paramBagPre;

                spec.addToXmlDocConversionDuration(System.nanoTime() - xmlConvStart);

                final Stream<FlattenerListItem> mapListItemStream = XmlFlattener_EnableParallelJavaLambdaStreams ? flattenerListItems.parallelStream() : flattenerListItems.stream();
                mapListItemStream.forEach(m-> ___processMapItemRow(m, paramBag2));

                if(rows % XMLFlattener_PropertyManager.XmlFlattener_PrintReportSize == 0)
                {
                    logger.info("Have now processed a total of " + (rows) + " rows for the extractor spec : " + spec.getName());
                }
            }
            logger.info("Now processed a total of " + rows + " | Closing all items ");
            for(FlattenerListItem m : flattenerListItems){
                m.close();
            }
        }
        catch(IOException e)
        {
            throw new RuntimeException("Encountered unexpected IO Exception - something's wrong with your file system : "+ e, e);
        }
    }

    private static Path getRelativeOrAbsolutePath(Path rootPath, String pathStr)
    {
        if(pathStr.startsWith("/")) return Paths.get(pathStr);
        return rootPath.resolve(pathStr);
    }

    private static void handleAndLogBrokenXMLInFeed(final Path rootPath,
                                                    final XmlFlattenerSpec spec,
                                                    final int rows,
                                                    final Exception ex,
                                                    final String s,
                                                    byte[] bytes)throws IOException{
        logger.warn(s + spec.getName() + " - exception was : " + ex, ex);
        logger.warn("Skipping the row");
        final Path xmlSubDir = getRelativeOrAbsolute(rootPath, XMLFlattener_PropertyManager.XmlFlattener_DumpXMLDirectory);

        if(Files.exists(xmlSubDir) == false) Files.createDirectories(xmlSubDir);
        Path xmlOutputFile = xmlSubDir.resolve(rows + "-broken.xml");
        logger.warn("Dumping the XML to disk : " + xmlOutputFile.toAbsolutePath());
        Files.write(xmlOutputFile, bytes);
    }

    private static void ___preProcessMapItemRow(FlattenerListItem m, ParameterBag paramBag)
    {
        m.preProcessRow(paramBag);
    }

    private static void ___processMapItemRow(FlattenerListItem m, ParameterBag paramBag)
    {
        m.processRow(paramBag);
    }

    public static void ___produceCSVExtracts(final Path rootPath,
                                             final Timestamp batchTime,
                                             final Map<String, XmlFlattenerSpec> extractorSpecs)
    {
        final List<Map.Entry<String, XmlFlattenerSpec>> extractorSpecSet = new ArrayList<>(extractorSpecs.entrySet());
        for(int i = 0 ; i < extractorSpecSet.size(); i++){
            final XmlFlattenerSpec spec = extractorSpecSet.get(i).getValue();
            logger.info("Extracting from the Spec : " + spec.getName() + " number " + i + " of " + extractorSpecSet.size() + " | (" + (((i*1.0)/ + extractorSpecSet.size())*100) + " %)");
        }
        logger.info("Done extracting all the " + extractorSpecSet.size() + " specs");
    }

    /**
     * Creates the level row contains based on this specification
     * @param mli
     */
    public static List<LayerRowsContainer> createLevelRowContainers(final FlattenerListItem mli, final boolean expandResolvedItems)
    {
        List<LayerRowsContainer> conts = new ArrayList<>();
        List<XmlFlattenerSpecColumn> columns = mli.getColumns();
        int level = 0;
        conts.add(LayerRowsContainer.create(0, mli.getMapName(), columns, expandResolvedItems));
        List<XmlFlattenerExplodeItem> explodeItems = mli.getExplodeItems();

        for(XmlFlattenerExplodeItem ei :explodeItems)
        {
            ___innerExplodeItemRecurse(ei, conts, expandResolvedItems);
        }

        return conts;
    }

    private static void ___innerExplodeItemRecurse(XmlFlattenerExplodeItem ei,
                                                   List<LayerRowsContainer> conts,
                                                   boolean expandResolvedItems)
    {
        final List<XmlFlattenerSpecColumn> columns = ei.getAllColumns();
        conts.add(LayerRowsContainer.create(ei.getLevel(), ei.getName(), columns, expandResolvedItems));

        List<XmlFlattenerExplodeItem> allExplodeItems = ei.getAllExplodeItems();
        for(XmlFlattenerExplodeItem i : allExplodeItems)
        {
            ___innerExplodeItemRecurse(i, conts, expandResolvedItems);
        }
    }

    private static final Path getRelativeOrAbsolute(Path rootPath, String pathStr){
        if(pathStr.startsWith("/")) return Paths.get(pathStr);
        else return rootPath.resolve(pathStr);
    }


    private static List<Path> getAllXmlFiles(Path XmlInputsDirectory) throws IOException
    {
        logger.info("Finding all the XML files in the " + XmlInputsDirectory.toAbsolutePath() + " directory.");

        if(Files.exists(XmlInputsDirectory) == false)
        {
            throw new RuntimeException("The path you specified " + XmlInputsDirectory.toAbsolutePath() + " does not exist.");
        }

        final Stream<Path> allPaths = Files.walk(XmlInputsDirectory);
        final List<Path> allXMLFiles = new ArrayList<>();

        allPaths.forEach(p->
                         {
                             if(p.getFileName().toString().endsWith(".xml") == true)
                             {
                                 allXMLFiles.add(p);
                             }
                         });
        logger.info("Found a total of " + allXMLFiles.size() + " to process. ");
        return allXMLFiles;
    }


}
