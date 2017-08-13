package net.johnewart.gearman.server.web;

import java.util.List;

import net.johnewart.gearman.engine.storage.ExceptionData;
import net.johnewart.gearman.engine.storage.ExceptionStorageEngine;

public class ExceptionsView {
	protected final ExceptionStorageEngine exceptionStorageEngine;
	private final int pageSize;
	private final int pageNum;
	private final int exceptionCount;

	public ExceptionsView(ExceptionStorageEngine exceptionStorageEngine,
		int pageSize, int pageNum) {
		this.exceptionStorageEngine = exceptionStorageEngine;
		this.pageSize = pageSize;
		this.pageNum = pageNum;
		this.exceptionCount = exceptionStorageEngine.getCount();
	}

	public int getPageSize() {
		return pageSize;
	}

	public int getPageNum() {
		return pageNum;
	}

	public int getPageCount() {
		return new Double(
			Math.ceil((double) exceptionCount / (double) pageSize)).intValue();
	}

	public int getNextPageNumber() {
		return pageNum + 1;
	}

	public int getPreviousPageNumber() {
		return pageNum - 1;
	}

	public boolean hasNextPage() {
		return (pageNum * pageSize < exceptionCount);
	}

	public boolean hasPreviousPage() {
		return pageNum > 1;
	}

	public int getPaginationStart() {
		if (getPageCount() < 10)
			return 1;
		else if (getPageCount() - pageNum < 10)
			return getPageCount() - 10;
		return pageNum;
	}

	public int getPaginationEnd() {
		int delta = getPageCount() - pageNum;
		if (delta > 10)
			return pageNum + 10;
		return pageNum + delta;
	}

	public int getExceptionCount() {
		return exceptionCount;
	}

	public int getStart() {
		return ((pageNum - 1) * pageSize) + 1;
	}

	public int getEnd() {
		int end = pageNum * pageSize;
		return end > exceptionCount ? exceptionCount : end;
	}

	public List<ExceptionData> getExceptions() {
		return exceptionStorageEngine.getExceptions(pageNum, pageSize);
	}

	public String getJobHandle(ExceptionData data) {
		return data.jobHandle;
	}

	public String getUniqueId(ExceptionData data) {
		return data.uniqueId;
	}

	public String getJobDataString(ExceptionData data) {
		return new String(data.jobData);
	}

	public String getExceptionDataString(ExceptionData data) {
		return new String(data.exceptionData);
	}

	public String getDateTime(ExceptionData data) {
		return data.when.toString();
	}
}
