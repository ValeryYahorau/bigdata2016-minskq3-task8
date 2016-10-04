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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Comparator.reverseOrder;
import static java.util.stream.Collectors.*;

public class SparkApp {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String FACEBOOK_TOKEN = "EAACEdEose0cBAODjSDumIec8GmsZAliFr3Gl5K8kC7tOyNY6g4orZANWln8J4LOsDfra5sDnK4wqM7mNijxXqIplZBXXgp8kozLZBqmyqGH4MtXq0HsUzsATbmQKISztsrsRI03tSEuHDKBsHf3knpWgqPvdGWYlEiWQhZBl9lQZDZD";


    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: SparkApp <file1> <file2>");
            System.exit(1);
        }
        //String filePath1 = args[0];
        //String filePath2 = args[1];
        String filePath1 = "hdfs://sandbox.hortonworks.com:8020/tmp/sparkhw1/in1.txt";
        String filePath2 = "hdfs://sandbox.hortonworks.com:8020/tmp/sparkhw1/in2.txt";
        String filePath3 = "hdfs://sandbox.hortonworks.com:8020/tmp/sparkhw1/in3.txt";


        SparkSession spark = SparkSession.builder().appName("Spark facebook integration App").config("spark.sql.warehouse.dir", "hdfs:///tmp/sparkhw1").getOrCreate();

        //TAGS
        JavaRDD<String> tagsRDD = spark.read().textFile(filePath2).javaRDD();
        JavaPairRDD<Long, List<String>> tagsIdsPairs = tagsRDD.mapToPair(new PairFunction<String, Long, List<String>>() {
            public Tuple2<Long, List<String>> call(String line) {
                String[] parts = line.split("\\s+");
                return new Tuple2<Long, List<String>>(Long.parseLong(parts[0]), Arrays.asList(parts[1].split(",")));
            }
        });
        Map<Long, List<String>> tagsMap = tagsIdsPairs.collectAsMap();

        //CITIES
        JavaRDD<String> citiesRDD = spark.read().textFile(filePath3).javaRDD();
        JavaPairRDD<Integer, String> citiesIdsPairs = citiesRDD.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String line) {
                String[] parts = line.split("\\s+");
                return new Tuple2<Integer, String>(Integer.parseInt(parts[0]), parts[1]);
            }
        });
        Map<Integer, String> citiesMap = citiesIdsPairs.collectAsMap();

        //LOGS with tags and cities
        JavaRDD<LogEntity> logEntitiesRDD = spark.read().textFile(filePath1).javaRDD().map(new Function<String, LogEntity>() {
            @Override
            public LogEntity call(String line) throws Exception {
                String[] parts = line.split("\\s+");

                LogEntity logEntity = new LogEntity();

                logEntity.setUserTagsId(Long.parseLong(parts[parts.length - 2]));
                List<String> tagsList = tagsMap.get(logEntity.getUserTagsId());
                logEntity.setTags(tagsList);

                logEntity.setCityId(Integer.parseInt(parts[parts.length - 15]));
                String city = citiesMap.get(logEntity.getCityId());
                logEntity.setCity(city);

                String dateInString = parts[1].substring(0, 8);
                logEntity.setDate(dateInString);
                return logEntity;
            }
        });
        Dataset<Row> logsDF = spark.createDataFrame(logEntitiesRDD, LogEntity.class);
        logsDF.createOrReplaceTempView("logs");
        logsDF.show();

        spark.stop();
//    }
//
//
//
//
//
//
//
//        //lines.
//        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) {
//                return Arrays.asList(SPACE.split(s)).iterator();
//            }
//        });
//
//        JavaPairRDD<String, Integer> ones = words.mapToPair(
//            new PairFunction<String, String, Integer>() {
//                @Override
//                public Tuple2<String, Integer> call(String s) {
//                    return new Tuple2<>(s, 1);
//                }
//            });
//
//        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
//            new Function2<Integer, Integer, Integer>() {
//                @Override
//                public Integer call(Integer i1, Integer i2) {
//                    return i1 + i2;
//                }
//            });
//
//        List<Tuple2<String, Integer>> output = counts.collect();
//        System.out.println("####1");
//        for (Tuple2<?, ?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }
//        System.out.println("####2");
//
//        List<String> keyWords = new ArrayList<>();
//        keyWords.add("minsk");
//
//        for (String kw : keyWords) {
//
//            ResultEntity resultEntity = new ResultEntity();
//            resultEntity.setKeyWord(kw);
//
//            FacebookClient facebookClient = new DefaultFacebookClient(FACEBOOK_TOKEN);
//            Connection<Event> eventConnections = facebookClient.fetchConnection("search", Event.class,
//                    Parameter.with("q", kw), Parameter.with("type", "event"), Parameter.with("fields", "id,name,description,attending_count"));
//
//            int sum = 0;
//
//            List<String> allWordsFromEventsDesc = new ArrayList<>();
//
//            for (List<Event> eventList : eventConnections) {
//                for (Event event : eventList) {
//
//                    sum = +event.getAttendingCount();
//
//                    if (StringUtils.isNotBlank(event.getDescription()) && StringUtils.isNotEmpty(event.getDescription())) {
//                        List<String> cuurentWordsList = Pattern.compile("\\W").splitAsStream(event.getDescription())
//                                .filter((s -> !s.isEmpty()))
//                                .filter(w -> !Pattern.compile("\\d+").matcher(w).matches())
//                                .collect(toList());
//                        allWordsFromEventsDesc.addAll(cuurentWordsList);
//                    }
//                    resultEntity.getEventIds().add(event.getId());
//                }
//            }
//
//            //TOTAL_AMOUNT_OF_VISITORS
//            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//            resultEntity.setTotalAmountAttendees(sum);
//
//            //TOKEN_MAP
//            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//            Map<String, Long> unsortedMap = allWordsFromEventsDesc.stream()
//                    .map(String::toLowerCase)
//                    .collect(groupingBy(Function.identity(), counting()))
//                    .entrySet().stream()
//                    .sorted(Map.Entry.<String, Long>comparingByValue(reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
//                    .limit(10)
//                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
//
//            Map<String, Long> sortedMap = unsortedMap.entrySet().stream()
//                    .sorted(Map.Entry.comparingByValue(reverseOrder()))
//                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
//
//            resultEntity.setTokenMap(sortedMap);
//
//
//            //Beside this collect all the attendees and visitors of this events and places by name with amount of occurrences;
//            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//            List<String> usersList = new ArrayList<>();
//            for (String eventId : resultEntity.getEventIds()) {
//                //System.out.println("$1_" + eventId);
//                Connection<User> attendesConncetions = facebookClient.fetchConnection(eventId + "/attending", User.class);
//                for (List<User> userList : attendesConncetions) {
//                    for (User user : userList) {
//                        usersList.add(user.getName());
//                    }
//                }
//            }
//
//            //Provide list of people sorted by occurrences sorted from largest to smallest.
//            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//            Map<String, Long> usersUnsortedMap = usersList.stream()
//                    .map(String::toLowerCase)
//                    .collect(groupingBy(Function.identity(), counting()))
//                    .entrySet().stream()
//                    .sorted(Map.Entry.<String, Long>comparingByValue(reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
//                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
//
//            Map<String, Long> userSortedMap = usersUnsortedMap.entrySet().stream()
//                    .sorted(Map.Entry.comparingByValue(reverseOrder()))
//                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
//
//            for (String str : userSortedMap.keySet()) {
//                System.out.println(str + "_" + userSortedMap.get(str));
//            }


        spark.stop();
    }

}