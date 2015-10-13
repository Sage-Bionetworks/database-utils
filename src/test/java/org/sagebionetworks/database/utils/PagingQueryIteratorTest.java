package org.sagebionetworks.database.utils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class PagingQueryIteratorTest {

	JdbcTemplate mockTempalte;
	RowMapper<Integer> mockRowMapper;
	List<Integer> tableData;
	String baseQuery;
	String expectedPageQuery;
	
	@SuppressWarnings("unchecked")
	@Before
	public void before(){
		mockTempalte = Mockito.mock(JdbcTemplate.class);
		mockRowMapper = Mockito.mock(RowMapper.class);
		baseQuery = "select * from foo";
		expectedPageQuery = baseQuery+" LIMIT ? OFFSET ?";
		int tableSize = 11;
		tableData = new ArrayList<Integer>(tableSize);
		// simulate a table of data
		for(int i=0; i<11; i++){
			tableData.add(i);
		}

		/*
		 * mock the template to return a subset of the table data based on the 
		 * requested limit and offset.
		 */
		doAnswer(new Answer<List<Integer>>() {
			@Override
			public List<Integer> answer(InvocationOnMock invocation)
					throws Throwable {
				String query = (String) invocation.getArguments()[0];
				assertEquals(expectedPageQuery, query);
				RowMapper<Integer> rowMapper = (RowMapper<Integer>) invocation.getArguments()[1];
				assertNotNull(rowMapper);
				// the last two args should be limit and offest.
				int argsSize = invocation.getArguments().length;
				Long limit = (Long) invocation.getArguments()[argsSize-2];
				Long offset = (Long) invocation.getArguments()[argsSize-1];
				if(offset >= tableData.size()){
					return new LinkedList<Integer>();
				}
				int requestedIndex = offset.intValue()+limit.intValue();
				// limit may go beyond the size of the list.
				int toIndex = Math.min(requestedIndex, tableData.size());
				// return a sublist of the table based on the limit and offset.
				return tableData.subList(offset.intValue(), toIndex);
			}
		}).when(mockTempalte).query(anyString(), any(RowMapper.class), Matchers.<Object>anyVararg());
	}
	
	@Test
	public void testFullReadMultiplePage(){
		long pageSize = 3;
		String argumentOne = "someArgument";
		int anotherArgument = 12;
		PagingQueryIterator<Integer> iterator = new PagingQueryIterator<Integer>(pageSize, mockTempalte, baseQuery, mockRowMapper, argumentOne, anotherArgument);
		// test by reading reading all of the data into a list
		List<Integer> results = new ArrayList<Integer>();
		while(iterator.hasNext()){
			results.add(iterator.next());
		}
		// The entire table data should now be in the results
		assertEquals(tableData.size(), results.size());
		assertEquals(tableData, results);
		// with a page size of 3, and a table size of 11, it should have taken 4 pages to read all of the data
		verify(mockTempalte, times(4)).query(anyString(), any(RowMapper.class), Matchers.<Object>anyVararg());
	}

	@Test
	public void testPageSizeSameAsTableSize(){
		long pageSize = tableData.size();
		String argumentOne = "someArgument";
		int anotherArgument = 12;
		PagingQueryIterator<Integer> iterator = new PagingQueryIterator<Integer>(pageSize, mockTempalte, baseQuery, mockRowMapper, argumentOne, anotherArgument);
		// test by reading reading all of the data into a list
		List<Integer> results = new ArrayList<Integer>();
		while(iterator.hasNext()){
			results.add(iterator.next());
		}
		// The entire table data should now be in the results
		assertEquals(tableData.size(), results.size());
		assertEquals(tableData, results);
		// when the page size is the same as the table size it takes two queries to find the end.
		verify(mockTempalte, times(2)).query(anyString(), any(RowMapper.class), Matchers.<Object>anyVararg());
	}
	
	@Test
	public void testPageSizeLargerThanTableSize(){
		long pageSize = tableData.size()+1;
		String argumentOne = "someArgument";
		int anotherArgument = 12;
		PagingQueryIterator<Integer> iterator = new PagingQueryIterator<Integer>(pageSize, mockTempalte, baseQuery, mockRowMapper, argumentOne, anotherArgument);
		// test by reading reading all of the data into a list
		List<Integer> results = new ArrayList<Integer>();
		while(iterator.hasNext()){
			results.add(iterator.next());
		}
		// The entire table data should now be in the results
		assertEquals(tableData.size(), results.size());
		assertEquals(tableData, results);
		// when the page size larger then the table size it should only take one query to get all the results.
		verify(mockTempalte, times(1)).query(anyString(), any(RowMapper.class), Matchers.<Object>anyVararg());
	}
	
	@Test
	public void testEmptyTable(){
		// start with an empty table.
		tableData.clear();
		long pageSize = tableData.size()+1;
		String argumentOne = "someArgument";
		int anotherArgument = 12;
		PagingQueryIterator<Integer> iterator = new PagingQueryIterator<Integer>(pageSize, mockTempalte, baseQuery, mockRowMapper, argumentOne, anotherArgument);
		// test by reading reading all of the data into a list
		List<Integer> results = new ArrayList<Integer>();
		while(iterator.hasNext()){
			results.add(iterator.next());
		}
		// The entire table data should now be in the results
		assertEquals(tableData.size(), results.size());
		assertEquals(tableData, results);
		// with an empty table it should only take one query.
		verify(mockTempalte, times(1)).query(anyString(), any(RowMapper.class), Matchers.<Object>anyVararg());
	}
}
