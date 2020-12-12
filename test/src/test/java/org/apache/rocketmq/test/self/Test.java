package org.apache.rocketmq.test.self;

import jdk.nashorn.internal.objects.annotations.Getter;

import java.util.Optional;

/**
 * @author yuanshancheng
 * @date 2020/12/12
 */
public class Test {
    public static void main(String[] args) {
        A a = new A();

        B b = new B();
//        a.setB(b);

//        b.setName("DSFDfdafdas");

        Optional<A> optional = Optional.of(a);
        System.out.println(optional.map(A::getB)
                .map(B::getName)
                .map(String::toLowerCase)
                .orElse("default"));

    }

    static class A {
        private B b;

        public B getB() {
            return b;
        }

        public void setB(B b) {
            this.b = b;
        }
    }

    static class B {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
