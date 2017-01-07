import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TriangleReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private void addNode(Map<Integer, Integer> nodeDegrees, int node) {
		Integer count = nodeDegrees.get(node);
		if (count == null) {
			nodeDegrees.put(node, 1);
		} else {
			nodeDegrees.put(node, count + 1);
		}
	}
	
	private void addNeighbour(HashMap<Integer, List<Integer>> nodeNeighbours,
			int node, int neighbour) {
		List<Integer> neighbours = nodeNeighbours.get(node);
		if (neighbours == null) {
			List<Integer> newNeighbours = new LinkedList<Integer>();
			newNeighbours.add(neighbour);
			nodeNeighbours.put(node, newNeighbours);
		} else {
			neighbours.add(neighbour);
			nodeNeighbours.put(node, neighbours);
		}
	}
	
	private void createMaps(Map<Integer, Integer> nodeDegrees,
			HashSet<String> edges,
			HashMap<Integer, List<Integer>> nodeNeighbours) {
		
		for (String edge : edges) {
			String[] nodes = edge.split("#");
			int leftNode = Integer.valueOf(nodes[0]);
			int rightNode = Integer.valueOf(nodes[1]);
			addNode(nodeDegrees, leftNode);
			addNode(nodeDegrees, rightNode);
			addNeighbour(nodeNeighbours, leftNode, rightNode);
			addNeighbour(nodeNeighbours, rightNode, leftNode);
		}
	}
	
	private boolean isHeavyHitterNode(HashSet<String> edges, int nodeDegree) {
		return ((double) nodeDegree) >= Math.sqrt(edges.size());
	}
	
	private String makeEdge(int node1, int node2) {
		return String.valueOf(node1) + "#" + String.valueOf(node2);
	}
	
	private List<String> findHeavyHitterTriangles(Map<Integer, Integer> nodeDegrees, 
		HashSet<String> edges) {
		HashSet<Integer> heavyHitterNodes = new HashSet<Integer>();
		for (Map.Entry<Integer, Integer> entry : nodeDegrees.entrySet()) {
			if (isHeavyHitterNode(edges, entry.getValue())) {
				heavyHitterNodes.add(entry.getKey());
			}
		}
		List<String> triangles = new LinkedList<String>();
		for (int firstNode : heavyHitterNodes) {
			for (int secondNode : heavyHitterNodes) {
				for (int thirdNode : heavyHitterNodes) {
					if (firstNode < secondNode && secondNode < thirdNode && 
						edges.contains(makeEdge(firstNode,secondNode)) && 
						edges.contains(makeEdge(secondNode, thirdNode)) && 
						edges.contains(makeEdge(firstNode, thirdNode))) {
						String triangle = StringUtils.join(new Object[] {
								firstNode, secondNode, thirdNode }, " ");
						triangles.add(triangle);
					}
				}
			}
		}
		return triangles;
	}
	
	private boolean isSmaller(Map<Integer, Integer> nodeDegrees, int node1, int node2) {
		int firstNodeDegree = nodeDegrees.get(node1);
		int secondNodeDegree = nodeDegrees.get(node2);
		return (firstNodeDegree == secondNodeDegree) ? 
			   (firstNodeDegree < secondNodeDegree) : (firstNodeDegree < secondNodeDegree);
	}
	
	private List<String> findOtherTriangles(Map<Integer, Integer> nodeDegrees,
			HashSet<String> edges,
			HashMap<Integer, List<Integer>> nodeNeighbours) {
		List<String> triangles = new LinkedList<String>();
		for(String edge : edges) {
			String[] nodes = edge.split("#");
			int leftNode = Integer.valueOf(nodes[0]);
			int rightNode = Integer.valueOf(nodes[1]);
			if((!isHeavyHitterNode(edges, nodeDegrees.get(leftNode)) ||
			   !isHeavyHitterNode(edges, nodeDegrees.get(rightNode))) &&
			   (isSmaller(nodeDegrees, leftNode, rightNode))) {
				List<Integer> neighbours = nodeNeighbours.get(leftNode);
				for(Integer neighbour : neighbours) {
					if (edges.contains(makeEdge(neighbour, rightNode))) {
						String triangle = 
								StringUtils.join(new Object[]{leftNode, neighbour, rightNode}, " ");
						triangles.add(triangle);
					}
				}
			}
		}
		return triangles;
	}
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		HashSet<String> edges = new HashSet<String>();
		for (Text edge : values) {
			edges.add(edge.toString());
		}
		Map<Integer, Integer> nodeDegrees = new HashMap<Integer, Integer>();
		HashMap<Integer, List<Integer>> nodeNeighbours = new HashMap<Integer, List<Integer>>();
		createMaps(nodeDegrees, edges, nodeNeighbours);
		List<String> triangles = findHeavyHitterTriangles(nodeDegrees, edges);
		triangles.addAll(findOtherTriangles(nodeDegrees, edges, nodeNeighbours));
		for (String triangle : triangles) {
			context.write(NullWritable.get(), new Text(triangle));
		}
	}
}