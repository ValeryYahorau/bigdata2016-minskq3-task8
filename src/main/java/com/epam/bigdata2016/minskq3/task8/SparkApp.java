package com.epam.bigdata2016.minskq3.task8;

import com.epam.bigdata2016.minskq3.task8.model.*;
import com.esotericsoftware.kryo.Kryo;
import com.restfb.*;
import com.restfb.types.Event;
import com.restfb.types.User;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Comparator.reverseOrder;


public class SparkApp {

    private static final String FACEBOOK_TOKEN = "EAAPP1tZBuTCoBAAmyoQMwtBH4hMvcZBd8mzqHjxUIOB1ob0DmCWAbFTlotZBw2QzVnXCnTu5J84IZBu4IsMitVFTKwI9DtwCGFmJZBVZBuZCc5zW7Ou9GCepv7ZAANQOoUulYuXlUZBTRRXjCkPe2JMBjx9iE7dvGv2IZD";
    private static final String UNKNOWN = "unknown";
    private static final String DEFAULT_DATE = "1990-01-01";
    private static final FacebookClient facebookClient = new DefaultFacebookClient(FACEBOOK_TOKEN, Version.VERSION_2_5);
    private static final SimpleDateFormat dt = new SimpleDateFormat("yyyy-mm-dd");

    public static class CustomKryoRegistrator implements KryoRegistrator {
        public void registerClasses(Kryo kryo) {
            kryo.register(DayCity.class);
            kryo.register(DayCityTag.class);
            kryo.register(FacebookAttendeeInfo.class);
            kryo.register(LogLineEntity.class);
            kryo.register(TagEvents.class);
            kryo.register(TagsLineEntity.class);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.err.println("Usage: SparkApp <warehouseDir> <file1> <file2> <file3>");
            System.exit(1);
        }
        String warehouseDir = args[0];
        String filePath1 = args[1];
        String filePath2 = args[2];
        String filePath3 = args[3];

//        String warehouseDir = "hdfs:///tmp/sparkhw1";
//        String filePath1 = "hdfs://sandbox.hortonworks.com:8020/tmp/sparkhw1/in1.txt";
//        String filePath2 = "hdfs://sandbox.hortonworks.com:8020/tmp/sparkhw1/in2.txt";
//        String filePath3 = "hdfs://sandbox.hortonworks.com:8020/tmp/sparkhw1/in3.txt";

        SparkSession spark = SparkSession.builder().appName("Spark facebook integration App")
                .config("spark.sql.warehouse.dir", warehouseDir)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", CustomKryoRegistrator.class.getName())
                .getOrCreate();

        //TAGS
        Dataset<String> data = spark.read().textFile(filePath2);
        String header = data.first();
        JavaRDD<String> tagsRDD = data.filter(x -> !x.equals(header)).javaRDD();
        JavaPairRDD<Long, Set<String>> tagsIdsPairs = tagsRDD.mapToPair((PairFunction<String, Long, Set<String>>) line -> {
            String[] parts = line.split("\\s+");
            return new Tuple2<Long, Set<String>>(Long.parseLong(parts[0]), new HashSet<String>(Arrays.asList(parts[1].split(","))));
        });
        Map<Long, Set<String>> tagsMap = tagsIdsPairs.collectAsMap();
        tagsRDD.unpersist();
        tagsIdsPairs.unpersist();
        data.unpersist();

        //CITIES
        Dataset<String> data2 = spark.read().textFile(filePath3);
        String header2 = data2.first();
        JavaRDD<String> citiesRDD = data2.filter(x -> !x.equals(header2)).javaRDD();
        JavaPairRDD<Integer, String> citiesIdsPairs = citiesRDD.mapToPair((PairFunction<String, Integer, String>) line -> {
            String[] parts = line.split("\\s+");
            return new Tuple2<Integer, String>(Integer.parseInt(parts[0]), parts[1]);
        });
        Map<Integer, String> citiesMap = citiesIdsPairs.collectAsMap();
        citiesRDD.unpersist();
        citiesIdsPairs.unpersist();
        data2.unpersist();

        //LOGS with tags and cities
        JavaRDD<LogLineEntity> logEntitiesRDD = spark.read().textFile(filePath1).javaRDD().map((Function<String, LogLineEntity>) line -> {
            String[] parts = line.split("\\s+");

            LogLineEntity logLineEntity = new LogLineEntity();
            logLineEntity.setUserTagsId(Long.parseLong(parts[parts.length - 2]));
            logLineEntity.setTags(tagsMap.get(logLineEntity.getUserTagsId()));
            logLineEntity.setCityId(Integer.parseInt(parts[parts.length - 15]));
            logLineEntity.setCity(citiesMap.get(logLineEntity.getCityId()));
            logLineEntity.setDate(parts[1].substring(0, 8));
            return logLineEntity;
        });

        //TASK1
        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        JavaPairRDD<DayCity, Set<String>> dayCityPrePairs =
                logEntitiesRDD.mapToPair((PairFunction<LogLineEntity, DayCity, Set<String>>) le -> {
                    DayCity dc = new DayCity();
                    dc.setCity(le.getCity());
                    dc.setDate(le.getDate());
                    return new Tuple2<DayCity, Set<String>>(dc, le.getTags());
                });
        JavaPairRDD<DayCity, Set<String>> dayCityPairs =
                dayCityPrePairs.reduceByKey((Function2<Set<String>, Set<String>, Set<String>>) (i1, i2) -> {
                    i1.addAll(i2);
                    return i1;
                });

        List<Tuple2<DayCity, Set<String>>> output = dayCityPairs.collect();
        System.out.println("### TASK1. Collect all unique keyword per day per location (city);");
        System.out.println("==================================================================");
        for (Tuple2<DayCity, Set<String>> tuple : output) {
            System.out.println("City : " + tuple._1().getCity() + " and date : " + tuple._1().getDate() + " have next tags: ");
            if (tuple._2 != null) {
                for (String tag : tuple._2()) {
                    System.out.print(tag + " ");
                }
            }
            System.out.println("\n==================================================================");
        }

        //TASK2
        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        JavaRDD<String> uniqueTagsRDD =
                logEntitiesRDD.flatMap((FlatMapFunction<LogLineEntity, String>) le -> le.getTags().iterator()).distinct();

        JavaRDD<TagEvents> tagsWithEventsRDD = uniqueTagsRDD.map(new Function<String, TagEvents>() {
            public TagEvents call(String tag) throws Exception {

                Connection<Event> eventConnections = facebookClient.fetchConnection("search", Event.class,
                        Parameter.with("q", tag), Parameter.with("type", "event"), Parameter.with("fields", "id,attending_count,place,name,description,start_time"));

                List<FacebookEventInfo> eventsPerTag = new ArrayList<FacebookEventInfo>();
                for (List<Event> eventList : eventConnections) {
                    for (Event event : eventList) {
                        if (event != null) {
                            FacebookEventInfo fe = new FacebookEventInfo(event.getId(), event.getName(), event.getDescription(), event.getAttendingCount(), tag);
                            if (event.getPlace() != null && event.getPlace().getLocation() != null && event.getPlace().getLocation().getCity() != null) {
                                fe.setCity(event.getPlace().getLocation().getCity());
                            } else {
                                fe.setCity(UNKNOWN);
                            }
                            if (event.getStartTime() != null) {
                                fe.setDate(dt.format(event.getStartTime()).toString());
                            } else {
                                fe.setDate(DEFAULT_DATE);
                            }

                            if (StringUtils.isNotEmpty(event.getDescription()) && StringUtils.isNotBlank(event.getDescription())) {
                                String[] words = event.getDescription().split("\\s+");

                                for (String word : words) {
                                    Integer f = fe.getWordsHistogram().get(word);
                                    if (f == null) {
                                        fe.getWordsHistogram().put(word, 1);
                                    } else {
                                        fe.getWordsHistogram().put(word, f + 1);
                                    }
                                }
                            }
                            eventsPerTag.add(fe);
                        }
                    }
                }

                TagEvents te = new TagEvents();
                te.setTag(tag);
                te.setEvents(eventsPerTag);

                return te;
            }
        });

        JavaRDD<FacebookEventInfo> allEventsRDD =
                tagsWithEventsRDD.flatMap((FlatMapFunction<TagEvents, FacebookEventInfo>) te -> te.getEvents().iterator());

        JavaPairRDD<DayCityTag, FacebookEventInfo> dayCityTagsPrePairs =
                allEventsRDD.mapToPair((PairFunction<FacebookEventInfo, DayCityTag, FacebookEventInfo>) fe -> {
                    DayCityTag dct = new DayCityTag();
                    dct.setCity(fe.getCity());
                    dct.setTag(fe.getTag());
                    dct.setDate(fe.getDate());
                    return new Tuple2<DayCityTag, FacebookEventInfo>(dct, fe);
                });

        JavaPairRDD<DayCityTag, FacebookEventInfo> dayCityTagsPairs =
                dayCityTagsPrePairs.reduceByKey((Function2<FacebookEventInfo, FacebookEventInfo, FacebookEventInfo>) (i1, i2) -> {
                    i1.setAttendingCount(i1.getAttendingCount() + i2.getAttendingCount());

                    Map<String, Integer> map1 = i1.getWordsHistogram();
                    for (String currentWord : i2.getWordsHistogram().keySet()) {
                        Integer f1 = map1.get(currentWord);
                        Integer f2 = i2.getWordsHistogram().get(currentWord);
                        if (f1 == null) {
                            map1.put(currentWord, f2);
                        } else {
                            map1.put(currentWord, f1 + f2);
                        }
                    }
                    i1.setWordsHistogram(map1);
                    return i1;
                });

        List<Tuple2<DayCityTag, FacebookEventInfo>> output2 = dayCityTagsPairs.collect();
        System.out.println("### TASK2. For each keyword per day per city store information like: KEYWORD DAY CITY TOTAL_AMOUNT_OF_VISITORS TOKEN_MAP(KEYWORD_1, AMOUNT_1... KEYWORD_N, AMOUNT_N). ");
        System.out.println("==================================================================");
        for (Tuple2<DayCityTag, FacebookEventInfo> tuple : output2) {

            System.out.println("KEYWORD : " + tuple._1().getTag() + " DAY : " + tuple._1().getDate() + " CITY : " + tuple._1().getCity());
            System.out.println("TOTAL_AMOUNT_OF_VISITORS : " + tuple._2.getAttendingCount());

            Map<String, Integer> sortedMap = tuple._2.getWordsHistogram().entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(reverseOrder())).limit(10)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

            System.out.println("TOKEN_MAP(KEYWORD_1, AMOUNT_1... KEYWORD_10, AMOUNT_10)  : ");
            for (String str : sortedMap.keySet()) {
                System.out.print(str + " " + sortedMap.get(str) + " | ");
            }
            System.out.println("\n==================================================================");
        }

        //TASK3
        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        JavaRDD<FacebookEventInfo> allEventsWithAttendees = allEventsRDD.map(new Function<FacebookEventInfo, FacebookEventInfo>() {
            public FacebookEventInfo call(FacebookEventInfo fei) throws Exception {

                System.out.println("%%%1 " + fei.getId());
                Connection<User> attendesConncetions = facebookClient.fetchConnection(fei.getId() + "/attending", User.class, Parameter.with("limit", 1000));
                List<FacebookAttendeeInfo> result = new ArrayList<FacebookAttendeeInfo>();
                for (List<User> userList : attendesConncetions) {
                    System.out.println("%%%2 " + userList.size());
                    for (User user : userList) {
                        FacebookAttendeeInfo fai = new FacebookAttendeeInfo(user.getName(), user.getId());
                        result.add(fai);
                    }
                }
                fei.setAttendees(result);
                return fei;
            }
        });

        JavaRDD<FacebookAttendeeInfo> allAttendeesRDD =
                allEventsWithAttendees.flatMap((FlatMapFunction<FacebookEventInfo, FacebookAttendeeInfo>) fei -> fei.getAttendees().iterator());

        JavaPairRDD<FacebookAttendeeInfo, Integer> allAttendesPairRDD =
                allAttendeesRDD.mapToPair((PairFunction<FacebookAttendeeInfo, FacebookAttendeeInfo, Integer>) fei -> new Tuple2<>(fei, 1));

        JavaPairRDD<FacebookAttendeeInfo, Integer> allAttendesCounts =
                allAttendesPairRDD.reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

        JavaRDD<FacebookAttendeeInfo> faiResultRDD =
                allAttendesCounts.map((Function<Tuple2<FacebookAttendeeInfo, Integer>, FacebookAttendeeInfo>) t -> {
                    t._1().setCount(t._2());
                    return t._1();
                });


        JavaRDD<FacebookAttendeeInfo> sortedAttendeesRDD =
                faiResultRDD.sortBy((Function<FacebookAttendeeInfo, Integer>) value -> value.getCount(), false, 1);

        Dataset<Row> allAttendeesDF = spark.createDataFrame(sortedAttendeesRDD, FacebookAttendeeInfo.class);
        allAttendeesDF.createOrReplaceTempView("allAttendees");
        System.out.println("### TASK3. Beside this collect all the attendees and visitors of this events and places by name with amount of occurrences; ");
        System.out.println("==================================================================");
        allAttendeesDF.show(20);

        spark.stop();
    }
}