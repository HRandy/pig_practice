package com.vpon.wizad.etl.pig;

import java.io.*;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class IsGuidInfoGood extends FilterFunc {
	@Override
	public Boolean exec(Tuple tuple) throws IOException {

		if (tuple == null || tuple.size() != 5) { // spec fields (31) + appended ad_network_id (1)
			return false;
		}
        try {
            String imei = (String)tuple.get(0);
            String idfa = (String)tuple.get(1);
            String macAddress = (String)tuple.get(2);
            Integer osId = (Integer)tuple.get(3);
            String osVersion = (String)tuple.get(4);

//            if (osId == null) {
//                return true; // just ignore it for now
//            }

            if (osId == 1) { // Android
                if (imei == null) {
                    return false;
                }
            } else if (osId == 2) { // IOS
                String firstDigit = osVersion.substring(0,1);

                if (firstDigit.equals("7")) {
                    if (idfa == null) {
                        return false;
                    }
                } else if (firstDigit.equals("6") || firstDigit.equals("5") || firstDigit.equals("4")) {
                    if (macAddress == null) {
                        return false;
                    }
                } else {
                    return false;
                }
            }

        } catch (Exception e){
            return false;
        }

        return true;
    }
}
