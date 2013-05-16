package com.xingcloud.qt.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.TreeMap;

public class DateManager {
    public static final Log LOG = LogFactory.getLog(DateManager.class);

    private final TimeZone TZ = TimeZone.getTimeZone("GMT+8");
    private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat tdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    private final SimpleDateFormat hdf = new SimpleDateFormat("HH");
    private final SimpleDateFormat minDf = new SimpleDateFormat("mm");
    private final SimpleDateFormat monDf = new SimpleDateFormat("yyyy-MM");
    private final SimpleDateFormat ydf = new SimpleDateFormat("yyyy");
    private final SimpleDateFormat hmdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    private final int TOTAL_MINS_PER_DAY = 24*60;

    private DateManager() {
        init();
    }

    private void init() {
        df.setTimeZone(TZ);
        tdf.setTimeZone(TZ);
        hdf.setTimeZone(TZ);
        minDf.setTimeZone(TZ);
        monDf.setTimeZone(TZ);
        ydf.setTimeZone(TZ);
    }

    private static final ThreadLocal instance = new ThreadLocal();

    public static synchronized DateManager getInstance() {
        DateManager manager = (DateManager) instance.get();
        if (manager == null) {
            manager = new DateManager();
            instance.set(manager);
        }
        return manager;
    }

    public boolean checkDate(String date) {
        return date != null && date.matches("^[0-9]{4}-[0-9]{2}-[0-9]{2}");
    }

    public String calDay(String date, int dis) throws ParseException{
        try {
            Date temp = new Date(getTimestamp(date));

            Calendar ca = Calendar.getInstance(TZ);
            ca.setTime(temp);
            ca.add(Calendar.DAY_OF_MONTH, dis);
            return df.format(ca.getTime());
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("CalDay got exception! " + date + " " + dis);
            throw new ParseException(date + " " + dis, 0);
        }
    }

    public int getGMTHour() {
        Calendar cal = Calendar.getInstance(TZ);
        hdf.setCalendar(cal);
        Date now = Calendar.getInstance().getTime();
        int hour = Integer.parseInt(hdf.format(now));
        return hour;
    }

    public int getGMTMins() {
        Calendar cal = Calendar.getInstance(TZ);
        minDf.setCalendar(cal);
        Date now = Calendar.getInstance().getTime();
        int mins = Integer.parseInt(minDf.format(now));
        return mins;
    }

    public String getTheWeek(String date) {
        Date temp = new Date(getTimestamp(date));
        Calendar ca = Calendar.getInstance();
        ca.setTime(temp);
        ca.set(Calendar.DAY_OF_WEEK, 1);
        String beginDate = df.format(ca.getTime());
        ca.set(Calendar.DAY_OF_WEEK, 7);
        String endDate = df.format(ca.getTime());
        return beginDate + " to " + endDate;
    }

    public String getTheMonth(String date) {
        Date temp = new Date(getTimestamp(date));
        return monDf.format(temp);
    }

    public long getTimestamp(String date) {
        String dateString = date + " 00:00:00";
        Date nowDate = null;
        try {
            nowDate = tdf.parse(dateString);
        } catch (ParseException e) {
            LOG.error("DateManager.daydis catch Exception with params is "
                    + date, e);
        }
        if (nowDate != null) {
            return nowDate.getTime();
        } else {
            return -1;
        }
    }

    public String getOneDay(long dis) {
        long oneDay = 24 * 60 * 60 * 1000;
        Date yesterday = new Date(System.currentTimeMillis() + dis * oneDay);
        return df.format(yesterday);
    }

    public int compareDate(String DATE1, String DATE2) throws ParseException{
        try {
            Date dt1 = df.parse(DATE1);
            Date dt2 = df.parse(DATE2);
            if (dt1.getTime() > dt2.getTime()) {
                return 1;
            } else if (dt1.getTime() < dt2.getTime()) {
                return -1;
            } else {
                return 0;
            }
        } catch (Exception e) {
            LOG.error("Invalid date format! Date1: " + DATE1 + "\tDate2: " + DATE2, e);
            e.printStackTrace();
            throw new ParseException(DATE1 + "\t" + DATE2, 0);
        }

    }

    public Date short2Date(String date) throws ParseException {
        return df.parse(date);
    }

    public int parseYear(String date) throws ParseException {
        return Integer.parseInt(ydf.format(short2Date(date)));
    }

    public int parseMonth(String date) throws ParseException {
        return Integer.parseInt(monDf.format(short2Date(date)));
    }

    public int parseDate(String date) throws ParseException {
        return Integer.parseInt(df.format(short2Date(date)));
    }

    public String dateAdd(String date, int offest) throws ParseException {
        if (offest == 0) {
            return date;
        }
        long oneDay = 24 * 60 * 60 * 1000;
        Date yesterday = new Date(df.parse(date).getTime() + offest * oneDay);
        return df.format(yesterday);
    }

    public String dateSubtraction(String date, int offest)
            throws ParseException {
        return dateAdd(date, -offest);
    }

    public TreeMap<Long, String> generatePeriodMap(String date, int period) {
        date = date.substring(0,4) + "-" + date.substring(4,6) + "-" + date.substring(6,8);

        TreeMap<Long, String> pm = new TreeMap<Long, String>();
        int i = 0;
        for (int time=0; time<24*60; time+=period) {
            int hour = time/60;
            int mins = time%60;
            String HH = (hour<10 ? "0"+hour : String.valueOf(hour));
            String mm = mins<10 ? "0"+mins : String.valueOf(mins);
            String dateTmp = date + " " + HH + ":" + mm;

            Date nowDate = null;
            try {
                nowDate = hmdf.parse(dateTmp);
            } catch (ParseException e) {
                LOG.error("DateManager.daydis catch Exception with params is "
                        + date, e);
            }
            long ts = nowDate.getTime();
            pm.put(ts, dateTmp);
        }
        return pm;
    }

    public String[] generatePeriodKeys(String date, int period) {
        if (!date.contains("-"))
            date = date.substring(0,4) + "-" + date.substring(4,6) + "-" + date.substring(6,8);

        String[] periodKeys = new String[24*60/period];
        int i = 0;
        for (int time=0; time<24*60; time+=period) {
            int hour = time/60;
            int mins = time%60;
            String HH = (hour<10 ? "0"+hour : String.valueOf(hour));
            String mm = mins<10 ? "0"+mins : String.valueOf(mins);
            String dateTmp = date + " " + HH + ":" + mm;

            periodKeys[i++] = dateTmp;
        }
        return periodKeys;
    }


}
