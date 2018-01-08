package assignment1;

public class ComputeFibonacci {

	public static int fibonacci(int n) {
		if(n <= 0)
			return 0;
		if(n == 1)
			return 1;
		else
			return fibonacci(n-1) + fibonacci(n-2);
	}
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//		for(int i = 0; i <= 17; i++){
//			fibonacci(i);
//		}
//	}

}
