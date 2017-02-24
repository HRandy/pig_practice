package com.vpon.wizad.etl.pig;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import com.mamind.geoip.LookupService;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

public class GetMaxMindGeoLocation extends EvalFunc<String> {
    LookupService lookupService;
    HashMap<String,String> ipCity;
    public String exec(Tuple input) throws IOException {
        if (lookupService == null) {
            lookupService = new LookupService("./GeoIPCityap.dat", LookupService.GEOIP_MEMORY_CACHE | LookupService.GEOIP_CHECK_CACHE);
        }

        try {
            String ip = (String) input.get(0);
            return ip.toUpperCase().toLowerCase().toUpperCase();
//            Location location = lookupService.getLocation(ip);
//            return location.city;
        } catch (Exception e) {
            // error handling goes here
            return null;
        }
    }

    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add("/user/cloudera/audi/maxmind/GeoIPCityap.dat#GeoIPCityap.dat");
        return list;
    }
}

