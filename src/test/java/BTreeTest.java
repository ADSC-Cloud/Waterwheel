import indexingTopology.util.BTree;

import java.util.List;

public class BTreeTest {
	public static void main(String args[]){
		IntegerBTree tree = new IntegerBTree(4);

        tree.insert(1);
        tree.insert(2);
        tree.insert(1);
        tree.insert(2);
        tree.insert(1);
        tree.insert(2);
        tree.insert(3);
        tree.insert(4);
        tree.insert(5);
        tree.insert(6);
        tree.insert(7);
        tree.insert(8);
        tree.insert(9);
        tree.insert(14);
        tree.insert(15);
        tree.insert(10);
        tree.insert(11);
        tree.insert(6);
        tree.insert(7);
        tree.insert(8);
        tree.insert(9);

        List<Integer> ret=tree.searchRange(2,13);
        for (Integer e : ret)
            System.out.println(e);


		return;
	}
}


class IntegerBTree extends BTree<Integer, Integer> {
    public IntegerBTree(int order) {
        super(order);
    }

    public void insert(int key) {
		this.insert(key, key);
	}
	
	public void remove(int key) {
		this.delete(key);
	}
}