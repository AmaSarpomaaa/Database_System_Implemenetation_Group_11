package index;

import java.util.ArrayList;
import java.util.List;

public class LeafNode extends Node {
    private List<Object> pointers;
    private LeafNode next;

    public LeafNode(int maxKeys) {
        super(maxKeys);
        this.pointers = new ArrayList<>();
        this.next = null;
    }

    public List<Object> getPointers() {
        return pointers;
    }

    public LeafNode getNext() {
        return next;
    }

    public void setNext(LeafNode next) {
        this.next = next;
    }

    public void insertSorted(Comparable<Object> key, Object pointer) {
        int i = 0;
        while (i < keys.size() && keys.get(i).compareTo(key) < 0) {
            i++;
        }

        keys.add(i, key);
        pointers.add(i, pointer);
    }
}