package uk.co.devworx.xmlflattener;

import org.graalvm.nativeimage.ImageSingletons;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeClassInitialization;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


class XmlFlattenerRunnerFeature implements Feature
{
    public void beforeAnalysis(BeforeAnalysisAccess access) {
        RuntimeClassInitialization.initializeAtBuildTime(Configuration.class.getPackage().getName());
        try {
            ImageSingletons.add(Configuration.class, Configuration.loadFromFile());
        } catch (Throwable ex) {
            throw new RuntimeException("native-image build-time configuration failed", ex);
        }
    }
}

/**
 * Main bootstrap class for executing the extraction
 */
public class XmlFlattenerRunner
{
    private static final Logger logger = Logger.getLogger(XmlFlattenerRunner.class);

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String... args) throws Exception
    {
     /*   Configuration configuration = ImageSingletons.lookup(Configuration.class);
        configuration.handler.handle();*/
        mainMain(args);
    }

    public static void mainMain(String... args) throws Exception
    {
        if (args.length < 1)
        {
            String msg = "The class expects 1 parameter: \n" +
                    "[0] - The YAML File describing the flattening criteria \n" +
                    "\nYou have specified: " + Arrays.toString(args);
            System.err.println(msg);
            System.exit(1);
            return;
        }

        final XmlFlattenerRunner runner = new XmlFlattenerRunner(Paths.get(args[0]));
        runner.execute();
    }

    private final Path spec;
    private final Timestamp batchTime;
    private final Map<String, XmlFlattenerSpec> extractorSpecs;

    public XmlFlattenerRunner(Path specP)
    {
        this.spec = specP;

        batchTime = new Timestamp(System.currentTimeMillis());
        logger.info("Reading from the specification : " + spec.toAbsolutePath());
        logger.debug("Batch time is " + batchTime);

        extractorSpecs = XmlFlattenerSpecFactory.parse(spec);

        logger.info("Read a total - " + extractorSpecs.size() + " | keys/names: " + extractorSpecs.keySet());
    }

    public void execute()
    {
         extractorSpecs.forEach((k,v) ->
        {
            XmlFlattener.fullyResolveDynamicColumns(batchTime, v);
        });

        extractorSpecs.forEach((k,v) ->
        {
            XmlFlattener.produceCSVFlattens(batchTime, v);
        });

        List<FlattenerListItem> mapItems = new ArrayList<>();

        extractorSpecs.values().forEach(s ->
                                        {
                                            s.getSpecListItems().values().forEach(mapItems::add);
                                        });

        for(FlattenerListItem mi : mapItems)
        {
            logger.info("Output Table - " + mi.getMapName() + " - " + mi.getCsvRowsWritten() + " CSV Rows - " + mi.getOutputCSVFile().toAbsolutePath());
        }
    }




}
