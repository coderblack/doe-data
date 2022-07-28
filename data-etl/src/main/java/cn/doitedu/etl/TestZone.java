package cn.doitedu.etl;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TestZone {

    public static void main(String[] args) {

        DateTimeFormatter formatter
                = DateTimeFormatter
                .BASIC_ISO_DATE;

        // create an ZonedDateTime object and
        ZonedDateTime zdt
                = ZonedDateTime
                .parse("2018-12-16",
                        formatter);

        System.out.println(zdt);


    }
}
