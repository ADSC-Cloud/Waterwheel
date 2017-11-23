package model;

import java.io.Serializable;

/**
 * Create by zelin on 17-11-8
 **/
public class PostPredicator implements Serializable {

    private String predicateName;
    private String predicateWay;
    private double predicateDigital;
    public PostPredicator(String predicateName, String predicateWay, double predicateDigital) {
        this.predicateName = predicateName;
        this.predicateWay = predicateWay;
        this.predicateDigital = predicateDigital;
    }

    public String getPredicateName() {
        return predicateName;
    }

    public String getPredicateWay() {
        return predicateWay;
    }

    public double getPredicateDigital() {
        return predicateDigital;
    }
}
