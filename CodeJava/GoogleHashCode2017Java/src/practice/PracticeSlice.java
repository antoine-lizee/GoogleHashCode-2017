package practice;
import java.util.Random;

public class PracticeSlice {
    public int r1, r2, c1, c2;
    
    public PracticeSlice(int r1, int c1, int r2, int c2) {
    	this.r1 = Math.min(r1, r2);
    	this.r2 = Math.max(r1, r2);
    	this.c1 = Math.min(c1, c2);
    	this.c2 = Math.max(c1, c2);
    }
    
    public String toString() {
    	return String.format("%d %d %d %d", r1, c1, r2, c2);
    }
    
    // the slice includes cells on all borders
    public int sliceSize() {
    	return (r2-r1+1)*(c2-c1+1);
    }
    
    public boolean containsCell(int r, int c) {
    	return r >= r1 && r <= r2 && c >= c1 && c <= c2;
    }
    
    public boolean overlap(PracticeSlice other) {
    	if(this.r2 < other.r1 || other.r2 < this.r1) {
    		return false;
    	}
    	
    	if(this.c2 < other.c1 || other.c2 < this.c1) {
    		return false;
    	}
    	
    	return true;
    }
   
    // returns a random valid slice or null
    // size <= H, and enough M and T
    public static PracticeSlice randomSlice(Random rn, PracticePizza pizza) {
    	// take a random slice 
    	int nRows = 1 + rn.nextInt(Math.min(pizza.R, pizza.H));
	    int nCols = 1 + rn.nextInt(Math.min(pizza.C, pizza.H/nRows));
	    
	    if(nRows * nCols > pizza.H) {
	    	return null;
	    }
	    
	    int r1 = rn.nextInt(pizza.R+1-nRows);
	    int r2 = r1 + nRows - 1;
	    int c1 = rn.nextInt(pizza.C+1-nCols);
	    int c2 = c1 + nCols - 1;
	    
	    if(r2 >= pizza.R || c2 >= pizza.C) {
	    	System.out.println("Random slice ends too far");
	    	return null;
	    	
	    }
	    
	    PracticeSlice slice = new PracticeSlice(r1, c1, r2, c2);
	    
	    if(pizza.countMushrooms(slice) < pizza.L) {
	    	return null;
	    }
	    
	    if(pizza.countTomatoes(slice) < pizza.L) {
	    	return null;
	    }
	    
	    return slice;

    }
    
}
