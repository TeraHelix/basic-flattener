package uk.co.devworx.xmlflattener;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

public class TimeUtilsTest
{

	@Test
	public void testTimeConversion()
	{
		Timestamp tst = TimeUtils.convertToSQLTimestamp("2019-09-01 15:01:00");
		Assertions.assertNotNull(tst);
	}


}
