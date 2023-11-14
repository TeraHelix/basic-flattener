package uk.co.devworx.xmlflattener;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import javax.xml.xpath.XPathExpressionException;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is responsible for creating a XmlFlattenerSpec from
 * an input YAML file.
 */
public class XmlFlattenerSpecFactory
{
	private static final Logger logger = Logger.getLogger(XmlFlattenerSpecFactory.class);

	private XmlFlattenerSpecFactory()
	{

	}

	private final static ObjectMapper mapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();

	static Yaml_Spec parseYamlSpec(Path yamlFile)
	{
		try(BufferedReader bufR = Files.newBufferedReader(yamlFile))
		{
			Yaml_Spec spec = mapper.readValue(bufR, Yaml_Spec.class);
			return spec;
		}
		catch (JsonMappingException | JsonParseException e)
		{
			String msg = "Unable to read the mapping value from the spec : " + e;
			logger.error(msg, e);
			throw new RuntimeException(msg,e);
		}
		catch (IOException e)
		{
			String msg = "Unable to read from the file : " + yamlFile + " - " + e;
			logger.error(msg, e);
			throw new RuntimeException(msg,e);
		}
	}

	public static Map<String, XmlFlattenerSpec> parse(Path yamlFile)
	{
		final Yaml_Spec yamlSpec = parseYamlSpec(yamlFile);
		final Path inputPath = yamlFile.getParent().resolve(yamlSpec.getInputPath());

		final Map<String, XmlFlattenerSpec> results = new LinkedHashMap<>();
		final AtomicInteger overallColumnOrderSeq = new AtomicInteger();

 		if (yamlSpec.getOutputTables() == null || yamlSpec.getOutputTables().isEmpty())
		{
			throw new RuntimeException("Your file : " + yamlFile.toAbsolutePath() + " - does not contain the top level field : " + "outputTables");
		}

		final List<Yaml_Spec_OutputTable> outputTables = yamlSpec.getOutputTables();
		for (Yaml_Spec_OutputTable outputTable : outputTables)
		{
			final String name = outputTable.getName();
			final Map<String, FlattenerListItem> mapListItems = new LinkedHashMap<>();

			Objects.requireNonNull(outputTable.getOutputFile(), "You must specify an output table in your configuration");

			final FlattenerListItem mli = FlattenerListItem.create(name,
																   outputTable.getOutputFile());
			mapListItems.put(mli.getMapName(), mli);

			//Now the columns
			final List<XmlFlattenerSpecColumn> allColumns = mli.getColumns();
			final List<XmlFlattenerExplodeItem> allExplodeItems = mli.getExplodeItems();
			final List<Yaml_Spec_Column> yamlColumns = outputTable.getDefinition();

			if (yamlColumns == null)
			{
				throw new RuntimeException("Unable to find the referenced map name - " + mli.getMapName() + " - this is for the new table name : " + name);
			}

			for (Yaml_Spec_Column yamlColumn : yamlColumns)
			{
				parseColumnsAndExplodeItem(1,
										   overallColumnOrderSeq,
										   yamlColumn,
										   Optional.empty(),
										   allColumns, allExplodeItems);
			}

			results.put(name, new XmlFlattenerSpec(yamlFile,
												   name,
												   mapListItems,
												   inputPath));
		}


		//Check for duplicate columns
		final StringBuilder duplicateReport = new StringBuilder();
		results.values().forEach(xmlSpec ->
								 {
									 xmlSpec.getSpecListItems().values().forEach(mli ->
																				 {
																					 final List<LayerRowsContainer> levelRowContainers = XmlFlattener.createLevelRowContainers(mli, false);
																					 List<XmlFlattenerSpecColumn> columns = LayerRowsContainer.getColumns(levelRowContainers);
																					 Set<String> columnsUnique = new HashSet<>();

																					 for (XmlFlattenerSpecColumn col : columns)
																					 {
																						 String colLowerCase = col.getColumnName().toLowerCase();
																						 if (columnsUnique.contains(colLowerCase) == true)
																						 {
																							 duplicateReport.append("The MapList - " + mli.getMapName() + " - contains a duplicate column - " + col.getColumnName() + " - this is not permitted. Please correct it. \n");
																						 } else
																						 {
																							 columnsUnique.add(colLowerCase);
																						 }
																					 }
																				 });
								 });
		if (duplicateReport.length() > 0)
		{
			throw new RuntimeException(duplicateReport.toString());
		}
		return Collections.unmodifiableMap(results);
	}


	private static void parseColumnsAndExplodeItem(final int layer,
												   final AtomicInteger overallColumnSeq,
												   final Yaml_Spec_Column column,
												   final Optional<XmlFlattenerExplodeItem> parent,
												   final List<XmlFlattenerSpecColumn> allColumns,
										   		   final List<XmlFlattenerExplodeItem> allExplodeItems)
	{
		if (column.getExplode() == null || column.getExplode() == false)
		{
			XmlFlattenerSpecColumn col = parseColumnSpec(layer, overallColumnSeq, parent, column);
			allColumns.add(col);
		}
		else
		{
			final String explodeItemName = column.getColumnName();
			final String explodeItemSource = column.getSourceDef();

			OptionalInt circuitBreaker = OptionalInt.empty();
			if (column.getCircuitBreaker() != null)
			{
				circuitBreaker = OptionalInt.of(column.getCircuitBreaker());
			}
			final XmlFlattenerExplodeItem childItem = XmlFlattenerExplodeItem.create(layer + 1,
																					 parent, explodeItemName,
																					 explodeItemSource,
																					 circuitBreaker);

			final List<XmlFlattenerSpecColumn> childAllColumns = childItem.getAllColumns();
			final List<XmlFlattenerExplodeItem> childAllExplodes = childItem.getAllExplodeItems();

			final List<Yaml_Spec_Column> repeatingList = column.getRepeatingList();
			if(repeatingList == null || repeatingList.isEmpty())
			{
				throw new RuntimeException("Unexpected node - " + column + " - did not contain a repeatinglist.  This is an exploded node - hence it must contain this field.");
			}

			for (Yaml_Spec_Column yaml_spec_column : repeatingList)
			{
				parseColumnsAndExplodeItem(layer + 1,
										   overallColumnSeq,
										   yaml_spec_column,
										   Optional.of(childItem),
										   childAllColumns,
										   childAllExplodes);
			}

			allExplodeItems.add(childItem);
		}

	}

	private static XmlFlattenerSpecColumn parseColumnSpec(final int level,
														  final AtomicInteger overallColumnOrderSeq,
														  final Optional<XmlFlattenerExplodeItem> parent,
														  final Yaml_Spec_Column column)
	{
		String columnName = column.getColumnName();
		String sourceTypeStr = column.getSourceType();
		String source = column.getSourceDef();

		String attFilter = column.getAttributeFilter();

		if (columnName == null)
			throw new RuntimeException("You have a missing column name in the json node : " + column);
		if (sourceTypeStr == null)
			throw new RuntimeException("You have a missing sourceType in the json node : " + column);
		if (source == null)
			throw new RuntimeException("You have a missing source in teh json node : " + column);

		try
		{
			XmlFlattenerSourceType sourceType = XmlFlattenerSourceType.valueOf(sourceTypeStr);
			return XmlFlattenerSpecColumn.create(level,
												 overallColumnOrderSeq.getAndIncrement(),
												 parent,
												 columnName,
												 sourceType,
												 source,
												 Optional.ofNullable(attFilter));
		}
		catch (IllegalArgumentException | XPathExpressionException e)
		{
			throw new RuntimeException("Invalid Source Type : " + sourceTypeStr + " - you specified something that is not in the enum");
		}
	}

}

/**
 * A surrogate class that will be used to parse the YAML
 */
class Yaml_Spec
{
	private String name;
	private List<Yaml_Spec_OutputTable> outputTables;
	private String inputPath;

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public List<Yaml_Spec_OutputTable> getOutputTables()
	{
		return outputTables;
	}

	public void setOutputTables(List<Yaml_Spec_OutputTable> outputTables)
	{
		this.outputTables = outputTables;
	}

	public String getInputPath()
	{
		return inputPath;
	}

	public void setInputPath(String inputPath)
	{
		this.inputPath = inputPath;
	}
}


class Yaml_Spec_OutputTable
{
	private String name;
	private String outputFile;
	private List<Yaml_Spec_Column> definition;

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public String getOutputFile()
	{
		return outputFile;
	}

	public void setOutputFile(String outputFile)
	{
		this.outputFile = outputFile;
	}



	public List<Yaml_Spec_Column> getDefinition()
	{
		return definition;
	}

	public void setDefinition(List<Yaml_Spec_Column> definition)
	{
		this.definition = definition;
	}
}

class Yaml_Spec_Column
{
	private String columnName;
	private String sourceType;
	private String sourceDef;
	private String attributeFilter;
	private Boolean explode;
	private List<Yaml_Spec_Column> repeatingList;
	private Integer circuitBreaker;

	public String getColumnName()
	{
		return columnName;
	}

	public void setColumnName(String columnName)
	{
		this.columnName = columnName;
	}

	public String getSourceType()
	{
		return sourceType;
	}

	public void setSourceType(String sourceType)
	{
		this.sourceType = sourceType;
	}

	public String getSourceDef()
	{
		return sourceDef;
	}

	public void setSourceDef(String sourceDef)
	{
		this.sourceDef = sourceDef;
	}

	public Boolean getExplode()
	{
		return explode;
	}

	public void setExplode(Boolean explode)
	{
		this.explode = explode;
	}

	public List<Yaml_Spec_Column> getRepeatingList()
	{
		return repeatingList;
	}

	public void setRepeatingList(List<Yaml_Spec_Column> repeatingList)
	{
		this.repeatingList = repeatingList;
	}

	public Integer getCircuitBreaker()
	{
		return circuitBreaker;
	}

	public void setCircuitBreaker(Integer circuitBreaker)
	{
		this.circuitBreaker = circuitBreaker;
	}

	public String getAttributeFilter()
	{
		return attributeFilter;
	}

	public void setAttributeFilter(String attributeFilter)
	{
		this.attributeFilter = attributeFilter;
	}
}




