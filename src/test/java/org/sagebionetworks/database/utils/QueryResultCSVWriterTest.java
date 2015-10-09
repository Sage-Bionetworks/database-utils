package org.sagebionetworks.database.utils;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.sagebionetworks.aws.utils.s3.ObjectCSVReader;

public class QueryResultCSVWriterTest {

	PagingQueryIterator<ExampleObject> mockIterator;
	List<ExampleObject> list;
	String fileName = "testFile";
	String[] headers = new String[]{"aString", "aLong", "aBoolean", "aDouble", "anInteger", "aFloat", "someEnum"};

	@Before
	public void before() {
		mockIterator = Mockito.mock(PagingQueryIterator.class);
		list = ExampleObject.buildExampleObjectList(5);
	}

	@Test
	public void test() throws IOException {
		Mockito.when(mockIterator.hasNext()).thenReturn(true, true, true, true, true, false);
		Mockito.when(mockIterator.next()).thenReturn(list.get(0), list.get(1), list.get(2), list.get(3), list.get(4), null);
		File file = null;
		ObjectCSVReader<ExampleObject> reader = null;
		try {
			file = QueryResultCSVWriter.write(mockIterator, fileName, headers, ExampleObject.class);
			reader = new ObjectCSVReader<ExampleObject>(new FileReader(file), ExampleObject.class, headers);
			List<ExampleObject> actual = new ArrayList<ExampleObject>();
			ExampleObject record = null;
			while ((record = reader.next()) != null) {
				actual.add(record);
			}
			assertEquals(new HashSet<ExampleObject>(list), new HashSet<ExampleObject>(actual));
		} finally {
			if (reader != null) reader.close();
			if (file != null) file.delete();
		}
	}

}
