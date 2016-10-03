package com.epam.bigdata2016.minskq3.task8;


import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.types.Event;
import com.restfb.types.User;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Comparator.reverseOrder;
import static java.util.stream.Collectors.*;

public class SparkApp {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String FACEBOOK_TOKEN = "EAACEdEose0cBAODjSDumIec8GmsZAliFr3Gl5K8kC7tOyNY6g4orZANWln8J4LOsDfra5sDnK4wqM7mNijxXqIplZBXXgp8kozLZBqmyqGH4MtXq0HsUzsATbmQKISztsrsRI03tSEuHDKBsHf3knpWgqPvdGWYlEiWQhZBl9lQZDZD";


    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder().appName("Spark facebook integration App").getOrCreate();

        //JavaRDD<String> distFile = sc.textFile("data.txt");
        JavaRDD<String> lines = spark.read().text(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        List<Tuple2<String, Integer>> output = counts.collect();
        System.out.println("####1");
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        System.out.println("####2");

        List<String> keyWords = new ArrayList<>();
        keyWords.add("minsk");

        for (String kw : keyWords) {

            ResultEntity resultEntity = new ResultEntity();
            resultEntity.setKeyWord(kw);

            FacebookClient facebookClient = new DefaultFacebookClient(FACEBOOK_TOKEN);
            Connection<Event> eventConnections = facebookClient.fetchConnection("search", Event.class,
                    Parameter.with("q", kw), Parameter.with("type", "event"), Parameter.with("fields", "id,name,description,attending_count"));

            int sum = 0;

            List<String> allWordsFromEventsDesc = new ArrayList<>();

            for (List<Event> eventList : eventConnections) {
                for (Event event : eventList) {

                    sum = +event.getAttendingCount();

                    if (StringUtils.isNotBlank(event.getDescription()) && StringUtils.isNotEmpty(event.getDescription())) {
                        List<String> cuurentWordsList = Pattern.compile("\\W").splitAsStream(event.getDescription())
                                .filter((s -> !s.isEmpty()))
                                .filter(w -> !Pattern.compile("\\d+").matcher(w).matches())
                                .collect(toList());
                        allWordsFromEventsDesc.addAll(cuurentWordsList);
                    }
                    resultEntity.getEventIds().add(event.getId());
                }
            }

            //TOTAL_AMOUNT_OF_VISITORS
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            resultEntity.setTotalAmountAttendees(sum);

            //TOKEN_MAP
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            Map<String, Long> unsortedMap = allWordsFromEventsDesc.stream()
                    .map(String::toLowerCase)
                    .collect(groupingBy(Function.identity(), counting()))
                    .entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue(reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
                    .limit(10)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            Map<String, Long> sortedMap = unsortedMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(reverseOrder()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

            resultEntity.setTokenMap(sortedMap);


            //Beside this collect all the attendees and visitors of this events and places by name with amount of occurrences;
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            List<String> usersList = new ArrayList<>();
            for (String eventId : resultEntity.getEventIds()) {
                //System.out.println("$1_" + eventId);
                Connection<User> attendesConncetions = facebookClient.fetchConnection(eventId + "/attending", User.class);
                for (List<User> userList : attendesConncetions) {
                    for (User user : userList) {
                        usersList.add(user.getName());
                    }
                }
            }

            //Provide list of people sorted by occurrences sorted from largest to smallest.
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            Map<String, Long> usersUnsortedMap = usersList.stream()
                    .map(String::toLowerCase)
                    .collect(groupingBy(Function.identity(), counting()))
                    .entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue(reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            Map<String, Long> userSortedMap = usersUnsortedMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(reverseOrder()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

            for (String str : userSortedMap.keySet()) {
                System.out.println(str + "_" + userSortedMap.get(str));
            }


            spark.stop();
        }
    }
}