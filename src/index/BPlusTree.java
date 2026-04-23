package index;

import buffer.BufferManager;
import storage.StorageManager;
import util.DBException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class BPlusTree {
    // Constants
    private static final byte INTERNAL  = 0;
    private static final byte LEAF      = 1;
    private static final int  NO_PAGE   = -1;

    private static final byte TYPE_INT  = 1;
    private static final byte TYPE_DBL  = 2;
    private static final byte TYPE_STR  = 3;
    private static final byte TYPE_BOOL = 4;

    // Fields
    private int rootPageId;
    private final int maxKeys;
    private final StorageManager storage;
    private final BufferManager  buffer;


    // Construction
    /**
     * Allocates one empty leaf page as the root.
     * Immediately call getRootPageId() and save the value in the catalog.
     */
    public BPlusTree(int maxKeys, StorageManager storage, BufferManager buffer)
            throws DBException {
        this.maxKeys  = maxKeys;
        this.storage  = storage;
        this.buffer   = buffer;
        this.rootPageId = storage.allocatePage();
        writeNode(rootPageId, new NodeContent(LEAF, NO_PAGE));
    }

    /**
     * Pass the rootPageId that was saved in the catalog
     */
    public BPlusTree(int rootPageId, int maxKeys,
                     StorageManager storage, BufferManager buffer) {
        this.rootPageId = rootPageId;
        this.maxKeys    = maxKeys;
        this.storage    = storage;
        this.buffer     = buffer;
    }

    public int getRootPageId() { return rootPageId; }

    public void insert(Comparable<Object> key, int dataPageId) throws DBException {
        Stack<Integer> path = new Stack<>();
        int leafId          = findLeafPageId(key, path);
        NodeContent leaf    = readNode(leafId);

        // Insert into sorted position in the leaf
        int i = 0;
        while (i < leaf.keys.size() && leaf.keys.get(i).compareTo(key) < 0) i++;
        leaf.keys.add(i, key);
        leaf.pointers.add(i, dataPageId);

        if (leaf.keys.size() > maxKeys) {
            splitLeaf(leafId, leaf, path);
        } else {
            writeNode(leafId, leaf);
        }
    }

    public int search(Comparable<Object> key) throws DBException {
        int leafId       = findLeafPageId(key, null);
        NodeContent leaf = readNode(leafId);
        for (int i = 0; i < leaf.keys.size(); i++) {
            if (leaf.keys.get(i).compareTo(key) == 0) {
                return leaf.pointers.get(i);
            }
        }
        return NO_PAGE;
    }

    public boolean delete(Comparable<Object> key) throws DBException {
        int leafId       = findLeafPageId(key, null);
        NodeContent leaf = readNode(leafId);
        for (int i = 0; i < leaf.keys.size(); i++) {
            if (leaf.keys.get(i).compareTo(key) == 0) {
                leaf.keys.remove(i);
                leaf.pointers.remove(i);
                writeNode(leafId, leaf);
                return true;
            }
        }
        return false;
    }

    public List<Integer> rangeSearch(Comparable<Object> lo,
                                     Comparable<Object> hi) throws DBException {
        List<Integer> results = new ArrayList<>();
        // Start from leftmost leaf if no lower bound
        int curId = (lo == null) ? leftmostLeafId() : findLeafPageId(lo, null);

        while (curId != NO_PAGE) {
            NodeContent leaf = readNode(curId);
            for (int i = 0; i < leaf.keys.size(); i++) {
                Comparable<Object> k = leaf.keys.get(i);
                if (lo != null && k.compareTo(lo) < 0) continue;
                if (hi != null && k.compareTo(hi) > 0)  return results;
                results.add(leaf.pointers.get(i));
            }
            curId = leaf.nextPageId;
        }
        return results;
    }

    private int findLeafPageId(Comparable<Object> key,
                               Stack<Integer> path) throws DBException {
        int curId = rootPageId;
        while (true) {
            NodeContent node = readNode(curId);
            if (node.type == LEAF) return curId;

            if (path != null) path.push(curId);

            int i = 0;
            while (i < node.keys.size() && key.compareTo(node.keys.get(i)) >= 0) i++;
            curId = node.pointers.get(i);
        }
    }

    private int leftmostLeafId() throws DBException {
        int curId = rootPageId;
        while (true) {
            NodeContent node = readNode(curId);
            if (node.type == LEAF) return curId;
            curId = node.pointers.get(0);
        }
    }

    private void splitLeaf(int leafId, NodeContent leaf,
                           Stack<Integer> path) throws DBException {
        int mid = leaf.keys.size() / 2;

        // Move the right half into a brand-new leaf
        NodeContent newLeaf = new NodeContent(LEAF, leaf.nextPageId);
        while (leaf.keys.size() > mid) {
            newLeaf.keys.add(leaf.keys.remove(mid));
            newLeaf.pointers.add(leaf.pointers.remove(mid));
        }

        int newLeafId      = storage.allocatePage();
        leaf.nextPageId    = newLeafId;

        writeNode(leafId,   leaf);
        writeNode(newLeafId, newLeaf);

        // Promote the first key of the new leaf up to the parent
        promote(newLeaf.keys.get(0), newLeafId, path);
    }

    private void splitInternal(int nodeId, NodeContent node,
                               Stack<Integer> path) throws DBException {
        int mid = node.keys.size() / 2;
        Comparable<Object> promotedKey = node.keys.get(mid);

        // Right half keys (after promoted key)
        NodeContent newNode = new NodeContent(INTERNAL, NO_PAGE);
        while (node.keys.size() > mid + 1) {
            newNode.keys.add(node.keys.remove(mid + 1));
        }
        node.keys.remove(mid); // remove promoted key from node

        // Right half pointers — newNode gets pointers from mid+1 onwards
        // node keeps pointers 0..mid (mid+1 pointers for mid keys) ✓
        // newNode gets pointers mid+1..end (one more than its keys) ✓
        while (node.pointers.size() > mid + 1) {
            newNode.pointers.add(node.pointers.remove(mid + 1));
        }

        int newNodeId = storage.allocatePage();
        writeNode(nodeId,    node);
        writeNode(newNodeId, newNode);

        promote(promotedKey, newNodeId, path);
    }

    private void promote(Comparable<Object> promotedKey, int rightChildId,
                         Stack<Integer> path) throws DBException {
        if (path.isEmpty()) {
            // build a new root above it
            int newRootId      = storage.allocatePage();
            NodeContent newRoot = new NodeContent(INTERNAL, NO_PAGE);
            newRoot.keys.add(promotedKey);

            // Left child is old root, right child is the new split node
            newRoot.pointers.add(rootPageId);
            newRoot.pointers.add(rightChildId);
            writeNode(newRootId, newRoot);
            rootPageId = newRootId;
            return;
        }

        int parentId         = path.pop();
        NodeContent parent   = readNode(parentId);

        // Insert promoted key and right child into parent in sorted order
        int i = 0;
        while (i < parent.keys.size()
                && promotedKey.compareTo(parent.keys.get(i)) > 0) i++;
        parent.keys.add(i, promotedKey);
        parent.pointers.add(i + 1, rightChildId);

        if (parent.keys.size() > maxKeys) {
            splitInternal(parentId, parent, path);
        } else {
            writeNode(parentId, parent);
        }
    }

    /**
     * Reads a node page from disk.
     */
    private NodeContent readNode(int pageId) throws DBException {
        byte[] data = storage.readPageBytes(pageId);
        return deserialize(data);
    }

    /**
     * Writes a node page to disk and marks it
     */
    private void writeNode(int pageId, NodeContent node) throws DBException {
        byte[] data = serialize(node);
        storage.writePageBytes(pageId, data);
        // do NOT call buffer.markDirty() — index pages manage their own persistence
    }

    /**
     * Serializes a node into bytes
     */
    private byte[] serialize(NodeContent node) {
        byte[] data = new byte[storage.getPageSize()];
        ByteBuffer buf = ByteBuffer.wrap(data);

        buf.put(node.type);
        buf.putInt(node.nextPageId);
        buf.putInt(node.keys.size());

        for (Comparable<Object> key : node.keys) writeKey(buf, key);

        for (int ptr : node.pointers) buf.putInt(ptr);

        return data;
    }

    private NodeContent deserialize(byte[] data) {
        ByteBuffer buf      = ByteBuffer.wrap(data);
        byte type           = buf.get();
        int  nextPageId     = buf.getInt();
        int  keyCount       = buf.getInt();

        NodeContent node = new NodeContent(type, nextPageId);

        for (int i = 0; i < keyCount; i++) node.keys.add(readKey(buf));

        int ptrCount = (type == LEAF) ? keyCount : keyCount + 1;
        for (int i = 0; i < ptrCount; i++) node.pointers.add(buf.getInt());

        return node;
    }

    private void writeKey(ByteBuffer buf, Comparable<Object> key) {
        Object raw = key;
        if (raw instanceof Integer) {
            buf.put(TYPE_INT);
            buf.putInt((Integer) raw);
        } else if (raw instanceof Double) {
            buf.put(TYPE_DBL);
            buf.putDouble((Double) raw);
        } else if (raw instanceof String) {
            String s = (String) raw;
            buf.put(TYPE_STR);
            buf.putInt(s.length());
            for (int i = 0; i < s.length(); i++) buf.put((byte) s.charAt(i));
        } else if (raw instanceof Boolean) {
            buf.put(TYPE_BOOL);
            buf.put((byte) (((Boolean) raw) ? 1 : 0));
        } else {
            // store as string
            String s = raw == null ? "" : raw.toString();
            buf.put(TYPE_STR);
            buf.putInt(s.length());
            for (int i = 0; i < s.length(); i++) buf.put((byte) s.charAt(i));
        }
    }

    @SuppressWarnings("unchecked")
    private Comparable<Object> readKey(ByteBuffer buf) {
        byte type = buf.get();
        switch (type) {
            case TYPE_INT:  return (Comparable<Object>) (Object) buf.getInt();
            case TYPE_DBL:  return (Comparable<Object>) (Object) buf.getDouble();
            case TYPE_STR: {
                int len    = buf.getInt();
                byte[] bytes = new byte[len];
                buf.get(bytes);
                return (Comparable<Object>) (Object) new String(bytes);
            }
            case TYPE_BOOL: return (Comparable<Object>) (Object) (buf.get() == 1);
            default: throw new IllegalStateException("Unknown key type byte: " + type);
        }
    }

    private static class NodeContent {
        byte type;
        int  nextPageId;  // leaf-chain link for leaves; unused (-1) for internal
        final List<Comparable<Object>> keys     = new ArrayList<>();
        final List<Integer>            pointers = new ArrayList<>();

        NodeContent(byte type, int nextPageId) {
            this.type       = type;
            this.nextPageId = nextPageId;
        }
    }
}