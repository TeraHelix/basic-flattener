package uk.co.devworx.xmlflattener;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class XmlFlattenerSpecFactoryTest
{
	private static final Logger logger = Logger.getLogger(XmlFlattenerSpecFactoryTest.class);

	private final Path spec_1 = Paths.get("src/test/resources/test-data/sample-data-1-spec.yml");
	private final Path spec_2 = Paths.get("src/test/resources/test-data/sample-data-explode-spec.yml");

	@Test
	public void testReadYamlSpec() throws Exception
	{
		Yaml_Spec yamlSpec = XmlFlattenerSpecFactory.parseYamlSpec(spec_1);
		Assertions.assertNotNull(yamlSpec);
		Assertions.assertEquals("Sample Data 1 Flattener", yamlSpec.getName());
	}

	@Test
	public void testReadFlattenerSpec() throws Exception
	{
		final Map<String, XmlFlattenerSpec> specs = XmlFlattenerSpecFactory.parse(spec_1);

		logger.info("Size of the specs : " + specs.size());

		Assertions.assertTrue(specs.isEmpty() == false);

		specs.forEach((k,v) ->
		{
			Assertions.assertNotNull(k);
			Assertions.assertNotNull(v);

			logger.info("k -> " + k);
			logger.info("v -> " + v);

		});
	}

	@Test
	public void testReadFlattenerExplodeSpec() throws Exception
	{
		final Map<String, XmlFlattenerSpec> specs = XmlFlattenerSpecFactory.parse(spec_2);
		logger.info("Size of the specs : " + specs.size());

		XmlFlattenerSpec xSpec = specs.get("table-explode");
		Map<String, FlattenerListItem> specListItems = xSpec.getSpecListItems();
		FlattenerListItem flattenerListItem = specListItems.get("table-explode");

		List<XmlFlattenerSpecColumn> columns = flattenerListItem.getColumns();
		List<XmlFlattenerExplodeItem> explodeItems = flattenerListItem.getExplodeItems();

		logger.info("EXPLODED SIZE : " + explodeItems.size());
		logger.info("COLUMNS SIZE : " + columns.size());

		for (XmlFlattenerSpecColumn column : columns)
		{
			logger.info(column);
		}

		for (XmlFlattenerExplodeItem exp : explodeItems)
		{
			logger.info(exp);
		}

	}




}
