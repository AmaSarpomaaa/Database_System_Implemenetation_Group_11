package index;

public class BPlusTree {
    private Node root;
    private final int maxKeys;

    public BPlusTree(int maxKeys) {
        this.maxKeys = maxKeys;
        this.root = new LeafNode(maxKeys);
    }

    public Node getRoot() {
        return root;
    }

    public LeafNode findLeaf(Comparable<Object> key) {
        Node current = root;

        while (current instanceof InternalNode internal) {
            int i = 0;
            while (i < internal.getKeys().size()
                    && key.compareTo(internal.getKeys().get(i)) >= 0) {
                i++;
            }
            current = internal.getChildren().get(i);
        }

        return (LeafNode) current;
    }

    public Object search(Comparable<Object> key) {
        LeafNode leaf = findLeaf(key);

        for (int i = 0; i < leaf.getKeys().size(); i++) {
            if (leaf.getKeys().get(i).compareTo(key) == 0) {
                return leaf.getPointers().get(i);
            }
        }

        return null;
    }

    public void insert(Comparable<Object> key, Object pointer) {
        LeafNode leaf = findLeaf(key);
        leaf.insertSorted(key, pointer);

        if (leaf.isFull()) {
            splitLeaf(leaf);
        }
    }

    private void splitLeaf(LeafNode leaf) {
        LeafNode newLeaf = new LeafNode(maxKeys);

        int mid = leaf.getKeys().size() / 2;

        while (leaf.getKeys().size() > mid) {
            newLeaf.getKeys().add(leaf.getKeys().remove(mid));
            newLeaf.getPointers().add(leaf.getPointers().remove(mid));
        }

        newLeaf.setNext(leaf.getNext());
        leaf.setNext(newLeaf);

        Comparable<Object> promotedKey = newLeaf.getKeys().get(0);

        if (leaf.getParent() == null) {
            InternalNode newRoot = new InternalNode(maxKeys);
            newRoot.getKeys().add(promotedKey);
            newRoot.getChildren().add(leaf);
            newRoot.getChildren().add(newLeaf);

            leaf.setParent(newRoot);
            newLeaf.setParent(newRoot);
            root = newRoot;
        } else {
            insertIntoParent(leaf.getParent(), promotedKey, newLeaf);
        }
    }

    private void insertIntoParent(InternalNode parent, Comparable<Object> key, Node rightChild) {
        parent.insertChild(key, rightChild);

        if (parent.isFull()) {
            splitInternal(parent);
        }
    }

    private void splitInternal(InternalNode node) {
        InternalNode newInternal = new InternalNode(maxKeys);

        int midIndex = node.getKeys().size() / 2;
        Comparable<Object> promotedKey = node.getKeys().get(midIndex);

        while (node.getKeys().size() > midIndex + 1) {
            newInternal.getKeys().add(node.getKeys().remove(midIndex + 1));
        }

        node.getKeys().remove(midIndex);

        while (node.getChildren().size() > midIndex + 1) {
            Node child = node.getChildren().remove(midIndex + 1);
            newInternal.getChildren().add(child);
            child.setParent(newInternal);
        }

        if (node.getParent() == null) {
            InternalNode newRoot = new InternalNode(maxKeys);
            newRoot.getKeys().add(promotedKey);
            newRoot.getChildren().add(node);
            newRoot.getChildren().add(newInternal);

            node.setParent(newRoot);
            newInternal.setParent(newRoot);
            root = newRoot;
        } else {
            newInternal.setParent(node.getParent());
            insertIntoParent(node.getParent(), promotedKey, newInternal);
        }
    }

    public void delete(Comparable<Object> key) {
        LeafNode curLeaf = findLeaf(key);
        for (int i = 0; i < curLeaf.getKeys().size(); i++) {
            if (curLeaf.getKeys().get(i).compareTo(key) == 0) {
                curLeaf.getKeys().remove(i);
                curLeaf.getPointers().remove(i);
                return;
            }
        }
    }
}