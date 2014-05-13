package simpledb;

public class TransPagePerm {
	private TransactionId tid;
	private PageId pid;
	private Permissions perm;
	
	public TransPagePerm(TransactionId tid, PageId pid, Permissions perm) {
		this.tid = tid;
		this.pid = pid;
		this.perm = perm;
	}
	
	public TransactionId getTid() {
		return this.tid;
	}
	
	public PageId getPid() {
		return this.pid;
	}
	
	public Permissions getPerm() {
		return this.perm;
	}
	
	public boolean equals(Object other) {
		if(other == null || !(other instanceof TransPagePerm)) {
			return false;
		} else {
			TransPagePerm othertpp = (TransPagePerm)other;
			if(tid.equals(othertpp.getTid()) && pid.equals(othertpp.getPid()) &&
					perm.equals(othertpp.getPerm())) {
				return true;
			}
			return false;
		}
	}
	
	public String toString() {
		return "(" + getTid() + ", " + getPid() + ", " + getPerm() + ")";
	}
}
