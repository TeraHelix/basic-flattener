package uk.co.devworx.xmlflattener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Generic utility for converting times.
 */
public class TimeUtils
{
	private static final Logger logger = LogManager.getLogger(TimeUtils.class);

	public static final List<DateTimeFormatter> DateTimeParseCandidates;

	static
	{
		List<DateTimeFormatter> preDateParseCandidates = new ArrayList<>();
		preDateParseCandidates.add(DateTimeFormatter.ISO_LOCAL_DATE);
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("yyyyMMdd"));
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("M/d/y H:m")); // for westpac - Calypso
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("M/d/y")); // for westpac - Calypso
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")); // for more clients
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("yyyy/M/dd HH:mm:ss")); // for more clients
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("yyyy/M/d HH:mm:ss")); // for more clients
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("yyyy/MM/d HH:mm:ss")); // for more clients
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("yyyy/M/d")); // for more clients
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("yyyy/MM/d")); // for more clients
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("yyyy/M/dd")); // for more clients
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("yyyy/MM/dd H:mm:ss")); // for more clients
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("yyyy/M/dd H:mm:ss")); // for more clients
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("yyyy/M/d H:mm:ss")); // for more clients
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("yyyy/MM/d H:mm:ss")); // for more clients
		preDateParseCandidates.add(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")); // for more clients


		DateTimeParseCandidates = Collections.unmodifiableList(preDateParseCandidates);
	}


	public static Timestamp convertToSQLTimestamp(String input)
	{
		try
		{
			LocalDateTime parsed = LocalDateTime.parse(input);
			return Timestamp.valueOf(parsed);
		}
		catch(Exception e)
		{
			logger.debug("Could not parse the input using the default method : " + input);
		}

		for (DateTimeFormatter dateFormat : DateTimeParseCandidates)
		{
			try
			{
				LocalDateTime localDate = LocalDateTime.parse(input, dateFormat);
				return Timestamp.valueOf(localDate);
			}
			catch(Exception e)
			{
				logger.debug(e);
				continue;
			}
		}

		throw new IllegalArgumentException("Unable to parse the input - " + input + " - to a timestamp. It is not in a format that is understood");

	}
}
