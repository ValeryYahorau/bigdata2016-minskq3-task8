package com.epam.bigdata2016.minskq3.task8;

import com.epam.bigdata2016.minskq3.task8.model.*;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Comparator.reverseOrder;


public class SparkApp {
    private static final String SPACE = " ";
    private static final String FACEBOOK_TOKEN = "EAACEdEose0cBAOwRnMA0oJxg1bbRG9uN2EjYcRbs4xsKo5X8ZBthlmndmDsWtjScf3wWZAVQXd06udWJ1MKhYUtmFX3V5Yox7WvwnjcESjBVJ0zBe4hKGV3ZAkUPyPZC0mQ63Iy0ZAm2FYKbGx9N9cFXgickzuISBjF5TecZC7KAZDZD";
    private static final String UNKNOWN = "unknown";
    private static final String DEFAULT_DATE = "2000-01-01";
    private static final FacebookClient facebookClient = new DefaultFacebookClient(FACEBOOK_TOKEN, Version.VERSION_2_5);
    private static final SimpleDateFormat dt = new SimpleDateFormat("yyyy-mm-dd");

    public static void main(String[] args) throws Exception {


//        if (args.length < 2) {
//            System.err.println("Usage: SparkApp <file1> <file2>");
//            System.exit(1);
//        }
        //String waregouseDir = args[0];
        //String filePath1 = args[1];
        //String filePath2 = args[2];
        //String filePath3 = args[3];
        String waregouseDir = "hdfs:///tmp/sparkhw1";
        String filePath1 = "hdfs://sandbox.hortonworks.com:8020/tmp/sparkhw1/in1.txt";
        String filePath2 = "hdfs://sandbox.hortonworks.com:8020/tmp/sparkhw1/in2.txt";
        String filePath3 = "hdfs://sandbox.hortonworks.com:8020/tmp/sparkhw1/in3.txt";


        SparkSession spark = SparkSession.builder().appName("Spark facebook integration App").config("spark.sql.warehouse.dir", waregouseDir).getOrCreate();

        //TAGS
        Dataset<String> data = spark.read().textFile(filePath2);
        String header = data.first();
        JavaRDD<String> tagsRDD = data.filter(x -> !x.equals(header)).javaRDD();
        JavaPairRDD<Long, List<String>> tagsIdsPairs = tagsRDD.mapToPair(new PairFunction<String, Long, List<String>>() {
            @Override
            public Tuple2<Long, List<String>> call(String line) {
                String[] parts = line.split("\\s+");
                return new Tuple2<Long, List<String>>(Long.parseLong(parts[0]), Arrays.asList(parts[1].split(",")));
            }
        });
        Map<Long, List<String>> tagsMap = tagsIdsPairs.collectAsMap();

        //CITIES
        Dataset<String> data2 = spark.read().textFile(filePath3);
        String header2 = data2.first();
        JavaRDD<String> citiesRDD = data2.filter(x -> !x.equals(header2)).javaRDD();
        JavaPairRDD<Integer, String> citiesIdsPairs = citiesRDD.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String line) {
                String[] parts = line.split("\\s+");
                return new Tuple2<Integer, String>(Integer.parseInt(parts[0]), parts[1]);
            }
        });
        Map<Integer, String> citiesMap = citiesIdsPairs.collectAsMap();

        //LOGS with tags and cities
        JavaRDD<LogLineEntity> logEntitiesRDD = spark.read().textFile(filePath1).javaRDD().map(new Function<String, LogLineEntity>() {
            @Override
            public LogLineEntity call(String line) throws Exception {
                String[] parts = line.split("\\s+");

                LogLineEntity logLineEntity = new LogLineEntity();

                logLineEntity.setUserTagsId(Long.parseLong(parts[parts.length - 2]));
                List<String> tagsList = tagsMap.get(logLineEntity.getUserTagsId());
                logLineEntity.setTags(tagsList);

                logLineEntity.setCityId(Integer.parseInt(parts[parts.length - 15]));
                String city = citiesMap.get(logLineEntity.getCityId());
                logLineEntity.setCity(city);

                String dateInString = parts[1].substring(0, 8);
                logLineEntity.setDate(dateInString);
                return logLineEntity;
            }
        });

        //TASK1
        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        JavaPairRDD<DayCity, List<String>> dayCityPrePairs = logEntitiesRDD.mapToPair(new PairFunction<LogLineEntity, DayCity, List<String>>() {
            @Override
            public Tuple2<DayCity, List<String>> call(LogLineEntity le) {
                DayCity dc = new DayCity();
                dc.setCity(le.getCity());
                dc.setDate(le.getDate());
                return new Tuple2<DayCity, List<String>>(dc, le.getTags());
            }
        });
        JavaPairRDD<DayCity, List<String>> dayCityPairs = dayCityPrePairs.reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
            @Override
            public List<String> call(List<String> i1, List<String> i2) {
                List<String> a1 = new ArrayList<>(i1);
                List<String> a2 = new ArrayList<>(i2);

                a1.removeAll(a2);
                a1.addAll(a2);
                return a1;
            }
        });

        List<Tuple2<DayCity, List<String>>> output = dayCityPairs.collect();
        System.out.println("### TASK1. Collect all unique keyword per day per location (city);");
        System.out.println("==================================================================");
        for (Tuple2<DayCity, List<String>> tuple : output) {
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
        JavaRDD<String> uniqueTagsRDD = logEntitiesRDD.flatMap(new FlatMapFunction<LogLineEntity, String>() {
            @Override
            public Iterator<String> call(LogLineEntity le) {
                return le.getTags().iterator();
            }
        }).distinct();

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

        JavaRDD<FacebookEventInfo> allEventsRDD = tagsWithEventsRDD.flatMap(new FlatMapFunction<TagEvents, FacebookEventInfo>() {
            @Override
            public Iterator<FacebookEventInfo> call(TagEvents te) {
                return te.getEvents().iterator();
            }
        });

        JavaPairRDD<DayCityTag, FacebookEventInfo> dayCityTagsPrePairs = allEventsRDD.mapToPair(new PairFunction<FacebookEventInfo, DayCityTag, FacebookEventInfo>() {
            @Override
            public Tuple2<DayCityTag, FacebookEventInfo> call(FacebookEventInfo fe) {
                DayCityTag dct = new DayCityTag();
                dct.setCity(fe.getCity());
                dct.setTag(fe.getTag());
                dct.setDate(fe.getDate());
                return new Tuple2<DayCityTag, FacebookEventInfo>(dct, fe);
            }
        });

        JavaPairRDD<DayCityTag, FacebookEventInfo> dayCityTagsPairs = dayCityTagsPrePairs.reduceByKey(new Function2<FacebookEventInfo, FacebookEventInfo, FacebookEventInfo>() {
            @Override
            public FacebookEventInfo call(FacebookEventInfo i1, FacebookEventInfo i2) {
                FacebookEventInfo fei = new FacebookEventInfo();
                fei.setAttendingCount(i1.getAttendingCount() + i2.getAttendingCount());

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
                fei.setWordsHistogram(map1);
                return fei;
            }
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
                Connection<User> attendesConncetions = facebookClient.fetchConnection(fei.getId() + "/attending", User.class);
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

        JavaRDD<FacebookAttendeeInfo> allAttendeesRDD = allEventsWithAttendees.flatMap(new FlatMapFunction<FacebookEventInfo, FacebookAttendeeInfo>() {
            @Override
            public Iterator<FacebookAttendeeInfo> call(FacebookEventInfo fei) {
                return fei.getAttendees().iterator();
            }
        });

        JavaPairRDD<FacebookAttendeeInfo, Integer> allAttendesPairRDD = allAttendeesRDD.mapToPair(new PairFunction<FacebookAttendeeInfo, FacebookAttendeeInfo, Integer>() {
            @Override
            public Tuple2<FacebookAttendeeInfo, Integer> call(FacebookAttendeeInfo fei) {
                return new Tuple2<>(fei, 1);
            }
        });

        JavaPairRDD<FacebookAttendeeInfo, Integer> allAttendesCounts = allAttendesPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        JavaRDD<FacebookAttendeeInfo> faiResultRDD = allAttendesCounts.map(new Function<Tuple2<FacebookAttendeeInfo, Integer>, FacebookAttendeeInfo>() {
            @Override
            public FacebookAttendeeInfo call(Tuple2<FacebookAttendeeInfo, Integer> t) throws Exception {
                t._1().setCount(t._2);
                return t._1();
            }
        });

        System.out.println("### TASK3. Beside this collect all the attendees and visitors of this events and places by name with amount of occurrences; ");
        System.out.println("==================================================================");
        List<FacebookAttendeeInfo> output3 = faiResultRDD.collect().subList(0, 10);
        for (FacebookAttendeeInfo fai : output3) {
            System.out.println("%%%3" + fai.getId() + " " + fai.getName() + " " + fai.getCount());
        }

//        Dataset<Row> allAttendeesDF = spark.createDataFrame(faiResultRDD, FacebookAttendeeInfo.class);
//        allAttendeesDF.createOrReplaceTempView("allAttendees");
//        allAttendeesDF.orderBy("count");


        //allAttendeesDF.limit(20).show();


        spark.stop();
    }

}

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