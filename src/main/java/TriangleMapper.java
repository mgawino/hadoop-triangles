import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TriangleMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final int N = 5;
	
	int hash(int value) {
		return value % N;
	}
	
	String makeReducerHash(int hash1, int hash2, int hash3) {
		return String.valueOf(hash1) + String.valueOf(hash2) + 
			   String.valueOf(hash3);
	}
	
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String nodes[] = value.toString().split(" ");
		int firstNode = Integer.valueOf(nodes[0]);
		int secondNode = Integer.valueOf(nodes[1]);
		if (firstNode > secondNode) {
			int tmp = firstNode;
			firstNode = secondNode;
			secondNode = tmp;
		}
		int firstNodeHash = hash(firstNode);
		int secondNodeHash = hash(secondNode);
		if (firstNodeHash > secondNodeHash) {
			int tmp = firstNodeHash;
			firstNodeHash = secondNodeHash;
			secondNodeHash = tmp;
		}
		String edge = String.valueOf(firstNode) + "#" + String.valueOf(secondNode);
		for (int a = 0; a < N; a++) {
			for (int b = a+1; b < N; b++) {
				for (int c = b+1; c < N; c++) {
					if ((firstNodeHash == a && secondNodeHash == b) ||
						(firstNodeHash == a && secondNodeHash == c) || 
						(firstNodeHash == b && secondNodeHash == c)) {
						Text reduceKey = new Text(makeReducerHash(a, b, c));
						context.write(reduceKey, new Text(edge));
					}
				}
			}
		}
	}
}