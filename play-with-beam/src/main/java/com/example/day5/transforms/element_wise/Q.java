package com.example.day5.transforms.element_wise;

import java.util.ArrayList;
import java.util.LinkedList;

/*

iterator => how to loop through the data from the data-structure
data-structure + iterator => iterable

 */

class MyList<E> implements Iterable<E> {
    Object[] elements;
    public MyList(int size) {
        elements = new Object[size];
    }
    public void add(int index, E element) {
        elements[index] = element;
    }
    public E get(int index) {
        return (E) elements[index];
    }

    @Override
    public java.util.Iterator<E> iterator() {
        return new java.util.Iterator<E>() {
            int index = 0;
            @Override
            public boolean hasNext() {
                return index < elements.length;
            }
            @Override
            public E next() {
                return (E) elements[index++];
            }
        };
    }

}

public class Q {
    public static void main(String[] args) {

        ArrayList<String> fruits1 = new ArrayList<>();
        fruits1.add("apple");
        fruits1.add("banana");
        fruits1.add("apricot");

        for (String word : fruits1) {
            System.out.println("Word: " + word);
        }

        LinkedList<String> fruits2 = new LinkedList<>();
        fruits2.add("apple");
        fruits2.add("banana");
        fruits2.add("cherry");

        for (String fruit : fruits2) {
            System.out.println("Fruit: " + fruit);
        }

        MyList<String> fruits3 = new MyList<>(3);
        fruits3.add(0, "apple");
        fruits3.add(1, "banana");
        fruits3.add(2, "cherry");

        for (String fruit : fruits3) {
            System.out.println("MyList Fruit: " + fruit);
        }

    }
}
