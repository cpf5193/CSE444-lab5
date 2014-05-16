package simpledb;

public class TidPid{
	private TransactionId tid;
	private PageId pid;
	
	public TidPid(TransactionId tid, PageId pid) {
		this.tid = tid;
		this.pid = pid;
	}
	
	public TransactionId getTid() {
		return this.tid;
	}
	
	public PageId getPid() {
		return this.pid;
	}
	public boolean equals(Object other) {
		if(other == null || !(other instanceof TidPid)) {
			return false;
		} else {
			TidPid othertpp = (TidPid)other;
			if(tid.equals(othertpp.getTid()) && pid.equals(othertpp.getPid())) {
				return true;
			}
			return false;
		}
	}
	
	public String toString() {
		return "(" + getTid() + ", " + getPid() + ")";
	}
}
