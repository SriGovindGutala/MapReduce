package com.erp;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String S = "hello,hello,hi,hi";
		String[] word;
		word = S.split(",");
        for (int i=0;i<word.length;i++){
        	System.out.println(word[i]);
        }
	}

}
