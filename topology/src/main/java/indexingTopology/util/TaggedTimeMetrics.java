package indexingTopology.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by robert on 7/7/17.
 */
public class TaggedTimeMetrics extends TimeMetrics implements Serializable {

    public Tags tags = new Tags();



    static Map<String, List<TimeMetrics>> group(String groupByTag, List<TaggedTimeMetrics> taggedTimeMetricsList) {
        boolean scalar = groupByTag == null || groupByTag.equals("");
        Map<String, List<TimeMetrics>> groupedTaggedTimeMetrics = new HashMap<>();
        if (scalar) {
            List<TimeMetrics> timeMetricsList = new ArrayList<>();
            timeMetricsList.addAll(taggedTimeMetricsList);
            groupedTaggedTimeMetrics.put("all", timeMetricsList);
        } else {
            taggedTimeMetricsList.forEach(t -> {
                groupedTaggedTimeMetrics.computeIfAbsent(t.tags.getTag(groupByTag), x ->new ArrayList<>()).add(t);
            });
        }
        return groupedTaggedTimeMetrics;
    }

    static Map<String, TimeMetrics> sum(String groupByTag, List<TaggedTimeMetrics> taggedTimeMetricsList) {
        Map<String, TimeMetrics> tagToAggregatedResult = new HashMap<>();
        Map<String, List<TimeMetrics>> groupedTaggedTimeMetrics = group(groupByTag, taggedTimeMetricsList);

        groupedTaggedTimeMetrics.forEach((tag, metrics) -> tagToAggregatedResult.put(tag, TimeMetrics.aggregate(metrics)));
        return tagToAggregatedResult;
    }

    static Map<String, TimeMetrics> average(String groupByTag, List<TaggedTimeMetrics> taggedTimeMetricsList) {
        Map<String, TimeMetrics> tagToAggregatedResult = new HashMap<>();
        Map<String, List<TimeMetrics>> groupedTaggedTimeMetrics = group(groupByTag, taggedTimeMetricsList);
        groupedTaggedTimeMetrics.forEach((tag, metrics) -> {
                    TimeMetrics avg = TimeMetrics.aggregate(metrics);
                    avg.metrics.keySet().forEach(k -> avg.metrics.compute(k, (k1, v) -> v / metrics.size()));
            tagToAggregatedResult.put(tag, avg);
        });
        return tagToAggregatedResult;
    }

    public static void main(String[] args) {
        TaggedTimeMetrics metrics = new TaggedTimeMetrics();
        TaggedTimeMetrics metrics1 = new TaggedTimeMetrics();
        TaggedTimeMetrics metrics2 = new TaggedTimeMetrics();
        metrics.metrics.put("event 1", 10.0);
        metrics.metrics.put("event 2", 8.0);
        metrics.metrics.put("event 3", 1.0);
        metrics.tags.setTag("location", "poor");


        metrics1.metrics.put("event 1", 3.0);
        metrics1.metrics.put("event 2", 1.0);
        metrics1.metrics.put("event 4", 2.0);
        metrics1.tags.setTag("location", "good");

        metrics2.metrics.put("event 1", 3.0);
        metrics2.metrics.put("event 2", 1.0);
        metrics2.metrics.put("event 4", 5.0);
        metrics2.tags.setTag("location", "good");

        List<TaggedTimeMetrics> list = new ArrayList<>();
        list.add(metrics);
        list.add(metrics1);
        list.add(metrics2);

        System.out.println(TaggedTimeMetrics.average("location", list));

    }
}
