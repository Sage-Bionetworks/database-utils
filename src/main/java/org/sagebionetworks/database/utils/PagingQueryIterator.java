package org.sagebionetworks.database.utils;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 * A wrapper for a non-paginated query that will convert the query to a
 * paginated from. All pages of the query results can then be access through the
 * iterator interfaces.
 * 
 * @param <T>
 *            The type of the query results.
 */
public class PagingQueryIterator<T> implements Iterator<T> {

	public static final String LIMIT_OFFSET = " LIMIT ? OFFSET ?";
	
	final Long pageSize;
	final JdbcTemplate template;
	final RowMapper<T> rowMapper;
	final String query;
	final Object[] args;
	boolean isLastPage;
	Iterator<T> currentPageIterator;

	/**
	 * Create a new iterator for each use.
	 * 
	 * @param pageSize
	 *            Sets the number of rows fetched for each page of the query.
	 * @param template
	 *            Template used to execute the query.
	 * @param query
	 *            The SQL query to page over. Note: The passed query must not
	 *            include limit or offset.
	 * @param rowMapper
	 * @param args
	 *            The arguments passed to the query. Note: The passed arguments
	 *            must not include limit or offset.
	 */
	public PagingQueryIterator(Long pageSize, JdbcTemplate template, String query,
			RowMapper<T> rowMapper, Object... args) {
		super();
		this.pageSize = pageSize;
		this.template = template;
		this.rowMapper = rowMapper;
		// Add limit and offset to the query.
		this.query = query + LIMIT_OFFSET;
		// add limit and offset to the args.
		this.args = ArrayUtils.addAll(args, pageSize, new Long(0));
		// query for the first page
		queryNextPage();
	}

	@Override
	public boolean hasNext() {
		// Is next result on the current page.
		if (currentPageIterator.hasNext()) {
			return true;
		}
		// all data from the current page has been read.
		// If this is the last page then done.
		if(isLastPage){
			return false;
		}
		// Need to fetch the next page from the DB.
		// Increment the offset and then get the next page.
		long offset = (Long) args[args.length - 1];
		offset += pageSize;
		args[args.length - 1] = offset;
		// query
		queryNextPage();
		return hasNext();
	}

	/**
	 * Query for the next page using the current arguments.
	 */
	private void queryNextPage() {
		List<T> page = template.query(query, rowMapper, args);
		// This is the last page if the size is less than the page size.
		isLastPage = page.size() < pageSize;
		currentPageIterator = page.iterator();
	}

	@Override
	public T next() {
		return currentPageIterator.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Not supported");
	}

}
