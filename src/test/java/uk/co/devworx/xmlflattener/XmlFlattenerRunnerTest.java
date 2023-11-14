package uk.co.devworx.xmlflattener;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class XmlFlattenerRunnerTest
{
	private static final Logger logger = Logger .getLogger(XmlFlattenerRunnerTest.class);

	@Test
	public void runFlatteningTest() throws Exception
	{
		String path = "src/test/resources/test-data/sample-data-1-spec.yml";
		runTest(path);
	}

	@Test
	public void runExplodeTest() throws Exception
	{
		String path = "src/test/resources/test-data/sample-data-explode-spec.yml";
		runTest(path);
	}

	private void runTest(String path) throws Exception
	{
		XmlFlattenerRunner.mainMain(path);
		Stream<Path> paths = Files.walk(Paths.get("target/sample-data-1"));

		paths.forEach(p -> {

			if(!p.getFileName().toString().endsWith(".csv")) return;

			try
			{
				String csv = new String(Files.readAllBytes(p));
				logger.debug("\n\n" + p.toAbsolutePath());
				logger.debug("\n\n" + csv);
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		});

	}

	@Test
	public void runDynAttributeTest() throws Exception
	{
		String path = "src/test/resources/test-data-dyn/sample-data-2-dyn-attrbs-spec.yml";
		XmlFlattenerRunner.mainMain(path);
		Stream<Path> paths = Files.walk(Paths.get("target/sample-data-dyn"));

		paths.forEach(p -> {

			if(!p.getFileName().toString().endsWith(".csv")) return;

			try
			{
				String csv = new String(Files.readAllBytes(p));
				logger.info("\n\n" + p.toAbsolutePath());
				logger.info("\n\n" + csv);
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		});
	}




}
