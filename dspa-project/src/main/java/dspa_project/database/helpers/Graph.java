package dspa_project.database.helpers;

import dspa_project.database.queries.SQLQuery;

import java.util.ArrayList;
import java.util.LinkedList;

public class Graph {
    public Node root;

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
        root = new Node(rootValue);
        int depth = 0;
        createAGraphForRoot(root, depth);
    }

    public void createAGraphForRoot(Node curNode, int depth){
        ArrayList<Long> descendants = SQLQuery.getSubclassesOfTagclass(curNode.value);
        for (int i = 0; i < descendants.size(); i++) {
            Node descendant = new Node(descendants.get(i), curNode, depth);
            curNode.children.add(descendant);
            createAGraphForRoot(descendant, depth+1);
        }
    }

    //TODO!
    public int findFirstCommonParent(long a, long b){
        int depth = 0;
        return depth;
    }
}
