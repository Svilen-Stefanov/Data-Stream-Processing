package dspa_project.database.helpers;

import dspa_project.database.queries.SQLQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import static java.lang.Math.max;

public class Graph {
    public Node root;
    private int maxDepth;
    HashMap<Long, Node> nodeMap;

    public class Node{
        int depth;
        long value;
        Node parent;
        LinkedList<Node> children;

        Node(long val){
            this.value = val;
            this.depth = 0;
            this.parent = null;
            this.children = new LinkedList<>();
        }

        Node(long val, Node parent, int depth){
            this.value = val;
            this.parent = parent;
            this.depth = depth;
            this.children = new LinkedList<>();
        }
    }

    // TODO: Make it generic by adding tablename, column names and use them in getSubclassesOfTagClass (also make this generic)
    public Graph(long rootValue){
        nodeMap = new HashMap<>();
        root = new Node(rootValue);
        int depth = 0;
        maxDepth = 0;
        nodeMap.put(rootValue, root);
        createAGraphForRoot(root, depth);
    }

    public void createAGraphForRoot(Node curNode, int depth){
        ArrayList<Long> descendants = SQLQuery.getSubclassesOfTagclass(curNode.value);
        for (int i = 0; i < descendants.size(); i++) {
            Node descendant = new Node(descendants.get(i), curNode, depth);
            curNode.children.add(descendant);
            nodeMap.put(descendant.value, descendant);
            createAGraphForRoot(descendant, depth+1);
        }
        maxDepth = max(depth, maxDepth);
    }

    public int distanceToCommonParent(long a, long b){
        int distance = 0;
        Node aNode = nodeMap.get(a);
        Node bNode = nodeMap.get(b);

        if (aNode == null || bNode == null){
            throw new IllegalArgumentException("Key not found. Tag does not exist in the graph.");
        }

        while ( aNode != bNode && aNode != null && bNode != null ){

            while( aNode != null && bNode != null && aNode.depth > bNode.depth ) {
                distance++;
                aNode = aNode.parent;
            }

            while( bNode != null && aNode != null && bNode.depth > aNode.depth ) {
                distance++;
                bNode = bNode.parent;
            }

            if (aNode != null && bNode != null && aNode != bNode) {
                aNode = aNode.parent;
                bNode = bNode.parent;
                distance++;
            }

        }

        return distance;
    }

    public int getMaxDepth() {
        return maxDepth;
    }
}
