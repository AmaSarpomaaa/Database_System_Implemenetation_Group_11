package index;

import java.util.ArrayList;
import java.util.List;

public abstract class Node {
    protected List<Comparable<Object>> keys;
    protected int maxKeys;
    protected InternalNode parent;

    public Node(int maxKeys) {
        this.maxKeys = maxKeys;
        this.keys = new ArrayList<>();
        this.parent = null;
    }

    public List<Comparable<Object>> getKeys() {
        return keys;
    }

    public InternalNode getParent() {
        return parent;
    }

    public void setParent(InternalNode parent) {
        this.parent = parent;
    }

    public boolean isFull() {
        return keys.size() > maxKeys;
    }

    public boolean isLeaf() {
        return this instanceof LeafNode;
    }
}