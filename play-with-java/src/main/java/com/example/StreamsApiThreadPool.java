package com.example;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

public class StreamsApiThreadPool {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        ForkJoinPool pool = new ForkJoinPool(2);
        pool.submit(()->{
            list.parallelStream()
                .map(i -> i * 2)
                .forEach(System.out::println);
        }).get();

        pool.shutdown();
        

    }
   
}
