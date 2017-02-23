package practice;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;


public class PracticePizza {
	
	public int R, C, L, H;
	public boolean[][] cells; // true means M (for mushroom) and false means T (tomato)
	
	public PracticePizza(int R, int C, int L, int H) {
		this.R = R;
		this.C = C;
		this.L = L;
		this.H = H;
		this.cells = new boolean[R][C];
	}
	
	/**
	 * Reads a picture from a .in file as described in the pdf 
	 * @param filename
	 * @throws IOException 
	 */
	public PracticePizza(File file) throws IOException {
		
		FileInputStream fstream = new FileInputStream(file);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		int numLine = -1;
		String strLine;

		while ((strLine = br.readLine()) != null)   {
		  if(numLine==-1) {
			  String[] ss = strLine.split(" ");
			  R = Integer.parseInt(ss[0]);
			  C = Integer.parseInt(ss[1]);
			  L = Integer.parseInt(ss[2]);
			  H = Integer.parseInt(ss[3]);
			  cells = new boolean[R][C];
		  } else {
			  boolean[] row = new boolean[strLine.length()]; // initialized to false
			  for(int i=0; i<strLine.length(); i++) {
				  if(strLine.charAt(i)=='M') {
					  row[i] = true;
				  } // else stay false for tomato
			  }
			  this.cells[numLine] = row;
		  }
		  numLine++;
		}

		//Close the input stream
		br.close();
	}
	
	public String toString() {
		StringBuilder sb =new StringBuilder();
		/*
		for(int i=0; i<numRows(); i++) {
			for(int j=0; j<numColumns(); j++) {
				if(this.pixels[i][j]) {
					sb.append("#");
				} else {
					sb.append(".");
				}
			}
			sb.append("\n");
		}
		*/
		sb.append(String.format("R=%d, C=%d, L=%d, H=%d", R, C, L, H));
		
		return sb.toString();
	}
	
	public int countMushrooms(PracticeSlice slice) {
		int n = 0;
		for(int r = slice.r1; r<= slice.r2; r++) {
			for(int c = slice.c1; c<= slice.c2; c++) {
    			if(this.cells[r][c]) {
    				n++;
    			}
    		}	
		}
		return n;
	}
	
	public int countTomatoes(PracticeSlice slice) {
		int n = 0;
		for(int r = slice.r1; r<= slice.r2; r++) {
			for(int c = slice.c1; c<= slice.c2; c++) {
    			if(!this.cells[r][c]) {
    				n++;
    			}
    		}	
		}
		return n;
	}
	
	
	public static void main(String[] args) throws IOException {
		File f = new File("practice_data/inputs/example.in");
		PracticePizza pizza = new PracticePizza(f);
		System.out.println(pizza);
	}
	
	public int maxScore() {
		return R*C;	
	}
	
}
