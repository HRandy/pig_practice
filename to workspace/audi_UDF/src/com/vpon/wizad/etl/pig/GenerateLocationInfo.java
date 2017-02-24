package com.vpon.wizad.etl.pig;

import java.io.IOException;

import com.vpon.wizad.etl.util.GenerateQuadKey;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import com.mamind.geoip.LookupService;
import com.mamind.geoip.Location;
import java.util.List;
import java.util.ArrayList;

public class GenerateLocationInfo extends EvalFunc<Tuple> {
    LookupService lookupService;
    GenerateQuadKey quadkeyGenerator;

    public Tuple exec(Tuple input) throws IOException {
        if (lookupService == null) {
            lookupService = new LookupService("./GeoIPCityap.dat",
                                                LookupService.GEOIP_MEMORY_CACHE | LookupService.GEOIP_CHECK_CACHE);
        }
        if (quadkeyGenerator == null) {
            quadkeyGenerator = new GenerateQuadKey();
        }

        try {
            Tuple output = TupleFactory.getInstance().newTuple();
            String ip = (String) input.get(0);
            if (ip.equals("NULL")) {
                return null;
            }
            Location location = lookupService.getLocation(ip);
            output.append(location.countryName);
            output.append(location.city);
            output.append(location.latitude);
            output.append(location.longitude);
            output.append(quadkeyGenerator.generateQuadKey(location.latitude, location.longitude));
            return output;
        } catch (Exception e) {
            return null;
        }
    }

    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add("/user/cloudera/audi/maxmind/GeoIPCityap.dat#GeoIPCityap.dat");
        return list;
    }
}

