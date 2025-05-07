package com.example.day5.transforms.agg;


import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.Comparator;

class NewProduct implements Serializable {
    private String name;
    private int price;
    private int discount;

    public NewProduct(String name, int price, int discount) {
        this.name = name;
        this.price = price;
        this.discount = discount;
    }

    public String getName() { return name; }
    public int getPrice() { return price; }
    public int getDiscount() { return discount; }

    @Override
    public String toString() {
        return name + " ($" + price + ", " + discount + "% off)";
    }
}


class MaxByPriceFn extends Combine.CombineFn<Product, Product, Product> {
    private final Comparator<Product> comparator;
    public MaxByPriceFn(Comparator<Product> comparator) {
        this.comparator = comparator;
    }
    @Override
    public Product createAccumulator() {
        return null;
    }
    @Override
    public Product addInput(Product accumulator, Product input) {
        if (accumulator == null || comparator.compare(input, accumulator) > 0) {
            return input;
        }
        return accumulator;
    }
    @Override
    public Product mergeAccumulators(Iterable<Product> accumulators) {
        Product best = null;
        for (Product p : accumulators) {
            if (best == null || comparator.compare(p, best) > 0) {
                best = p;
            }
        }
        return best;
    }
    @Override
    public Product extractOutput(Product accumulator) {
        return accumulator;
    }
}




public class MaxExample {

    public static <PCollection> void main(String[] args) {


        // Create a pipeline
        PipelineOptions options = PipelineOptionsFactory.create();
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);

        // Create a PCollection of NewProduct objects
        org.apache.beam.sdk.values.PCollection<NewProduct> products = pipeline.apply(Create.of(
                new NewProduct("Product A", 100, 10),
                new NewProduct("Product B", 200, 20),
                new NewProduct("Product C", 150, 15)
        ));

        // Find the product with the maximum price


    }

}
