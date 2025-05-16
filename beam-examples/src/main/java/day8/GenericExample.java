package day8;


class Box<E> {
    private E value;

    public void setValue(E value) {
        this.value = value;
    }

    public E getValue() {
        return value;
    }
}


public class GenericExample {
    public static void main(String[] args) {

        Box<Integer> numberBox = new Box<>();
        numberBox.setValue(10);

        //numberBox.setValue(true);

        int value = (int) numberBox.getValue();
        System.out.println("Value in the box: " + value);


        Box<String> stringBox = new Box<>();
        stringBox.setValue("Hello, World!");

        String strValue = (String) stringBox.getValue();
        System.out.println("String in the box: " + strValue);

    }
}
