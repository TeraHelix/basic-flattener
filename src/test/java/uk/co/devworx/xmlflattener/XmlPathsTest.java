package uk.co.devworx.xmlflattener;

import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;

/**
 * Some general tests for XML Paths
 */
public class XmlPathsTest
{
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(XmlPathsTest.class);

	@Test
	public void testXmlPaths() throws Exception
	{
		final Document doc =  ParameterBag.builder.parse("src/test/resources/test-data/sample-data-1-document-type-1.xml");

		String xpath1 = "root/element-3/element-3-sub";
		String xpath2 = "text()";

		XPathExpression xpath1_comp = XmlFlattenerSpec.xPath.compile(xpath1);
		XPathExpression xpath2_comp = XmlFlattenerSpec.xPath.compile(xpath2);

		NodeList eval1 = (NodeList) xpath1_comp.evaluate(doc, XPathConstants.NODESET);

		Assertions.assertEquals(4, eval1.getLength());

		for (int i = 0; i < eval1.getLength() ; i++)
		{
			String eval2 = (String)xpath2_comp.evaluate(eval1.item(i), XPathConstants.STRING);
			logger.debug("eval - [" + i + "] - " + eval2);
		}
	}

	@Test
	public void testXmlPathsAttributes() throws Exception
	{
		final Document doc =  ParameterBag.builder.parse("src/test/resources/test-data/sample-data-1-document-type-1.xml");

		String xpath1 = "root/element-2/element-2-tag-name";
		String xpath2 = "@key-1";

		XPathExpression xpath1_comp = XmlFlattenerSpec.xPath.compile(xpath1);
		XPathExpression xpath2_comp = XmlFlattenerSpec.xPath.compile(xpath2);

		NodeList eval1 = (NodeList) xpath1_comp.evaluate(doc, XPathConstants.NODESET);

		Assertions.assertEquals(4, eval1.getLength());

		for (int i = 0; i < eval1.getLength() ; i++)
		{
			String eval2 = (String)xpath2_comp.evaluate(eval1.item(i), XPathConstants.STRING);
			logger.debug("eval - [" + i + "] - " + eval2);
		}

	}

}
