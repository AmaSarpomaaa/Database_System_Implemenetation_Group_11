package index;

import java.util.ArrayList;
import java.util.List;

public class InternalNode extends Node {
    private List<Node> children;

    public InternalNode(int maxKeys) {
        super(maxKeys);
        this.children = new ArrayList<>();
    }

    public List<Node> getChildren() {
        return children;
    }

    public void insertChild(Comparable<Object> key, Node rightChild) {
        int i = 0;
        while (i < keys.size() && keys.get(i).compareTo(key) < 0) {
            i++;
        }

        keys.add(i, key);
        children.add(i + 1, rightChild);
        rightChild.setParent(this);
    }
}