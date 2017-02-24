package com.vpon.wizad.etl.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.vpon.wizad.etl.util.QuadkeyTemplateDB;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

public class GetGeoLocations extends EvalFunc<DataBag> {
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();
    QuadkeyTemplateDB qkDB;
    String includedRegions;

    public GetGeoLocations(String includedRegions) {
        super();
        this.includedRegions = includedRegions;
    }

//    public GetGeoLocations() {
//        super();
//    }

    public DataBag exec(Tuple input) throws IOException {
        if (qkDB == null) {
            qkDB = new QuadkeyTemplateDB("./qk_cn.csv",includedRegions);
        }

        DataBag output = mBagFactory.newDefaultBag();
        try {
            if (input == null || input.get(0) == null) {
                return output;
            }
            String quadkey = (String) input.get(0);
            List<String> found = qkDB.lookupRegions(quadkey);
            for (String regionName: found) {
                output.add(mTupleFactory.newTuple(regionName));
            }
            return output;
        } catch (Exception e) {
            e.printStackTrace();
            return output;
        }
    }

    public Schema outputSchema(Schema input) {
        try{
            Schema bagSchema = new Schema();
            bagSchema.add(new Schema.FieldSchema("region", DataType.CHARARRAY));

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                    bagSchema, DataType.BAG));
        }catch (Exception e){
            return null;
        }
    }

    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add("/user/cloudera/audi/region_template/qk_cn.csv#qk_cn.csv");
        return list;
    }


}

