package com.jointhegrid.udf.geoip;

import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.Country;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.Set;
import java.util.Arrays;

/**
 * GenericUDFGeoIP is a Hive User Defined Function that allows you to lookup
 * database information on a given ip.
 * argument 0 should be an IP string
 * argument 1 should be one of the following values:
 * COUNTRY_NAME, COUNTRY_CODE ,AREA_CODE
 * CITY,DMA_CODE,LATITUDE,LONGITUDE,METRO_CODE,POSTAL_CODE, REGION, ORG, ID
 * argument 2 should be the filename for you geo-ip database
 *
 * <pre>
 * Usage:
 * add file GeoIP.dat;
 * add jar geo-ip-java.jar;
 * add jar hive-udf-geo-ip-jtg.jar;
 * create temporary function geoip as 'com.jointhegrid.hive.udf.GenericUDFGeoIP';
 * select geoip(first, 'COUNTRY_NAME',  './GeoIP.dat' ) from a;
 * </pre>
 * @author ecapriolo
 */

@Description(
  name = "geoip",
  value = "_FUNC_(ip,property,database) - loads database into GEO-IP lookup "+
  "service, then looks up 'property' of ip. "
)

public class GenericUDFGeoIP extends GenericUDF {

  private String ipString = null;
  private Long ipLong = null;
  private String property;
  private String database;
  private LookupService ls;

  private static final String COUNTRY_NAME = "COUNTRY_NAME";
  private static final String COUNTRY_CODE = "COUNTRY_CODE";
  private static final String AREA_CODE    = "AREA_CODE";
  private static final String CITY         = "CITY";
  private static final String DMA_CODE     = "DMA_CODE";
  private static final String LATITUDE     = "LATITUDE";
  private static final String LONGITUDE    = "LONGITUDE";
  private static final String METRO_CODE   = "METRO_CODE";
  private static final String POSTAL_CODE  = "POSTAL_CODE";
  private static final String REGION       = "REGION";
  private static final String ORG          = "ORG";
  private static final String ID           = "ID";

  private static final Set<String> COUNTRY_PROPERTIES =
	  new CopyOnWriteArraySet<String>(Arrays.asList(
				  new String[] {COUNTRY_NAME, COUNTRY_CODE}));

  private static final Set<String> LOCATION_PROPERTIES =
	  new CopyOnWriteArraySet<String>(Arrays.asList(
				  new String[] {AREA_CODE, CITY, DMA_CODE, LATITUDE, LONGITUDE, METRO_CODE, POSTAL_CODE, REGION}));

  PrimitiveObjectInspector [] argumentOIs;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
          throws UDFArgumentException {

    argumentOIs = new PrimitiveObjectInspector [arguments.length];

    if ( arguments.length != 3) {
      throw new UDFArgumentLengthException(
              "The function GenericUDFGeoIP( 'input', 'resultfield', 'datafile' ) "
              + " accepts 3 arguments.");
    }

    if (!(arguments[0] instanceof StringObjectInspector) && !(arguments[0] instanceof LongObjectInspector)) {
      throw new UDFArgumentTypeException(0,
               "The first 3 parameters of GenericUDFGeoIP('input', 'resultfield', 'datafile')"
               + " should be string.");
    }
    argumentOIs[0] = (PrimitiveObjectInspector) arguments[0];

    for (int i = 1; i < arguments.length; i++) {
      if (!(arguments[i] instanceof StringObjectInspector )) {
        throw new UDFArgumentTypeException(i,
                "The first 3 parameters of GenericUDFGeoIP('input', 'resultfield', 'datafile')"
                + " should be string.");
      }
      argumentOIs[i] = (StringObjectInspector) arguments[i];
    }
    return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
            PrimitiveCategory.STRING);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (argumentOIs[0] instanceof LongObjectInspector) {
      this.ipLong = ((LongObjectInspector)argumentOIs[0]).get(arguments[0].get());
    } else {
      this.ipString = ((StringObjectInspector)argumentOIs[0]).getPrimitiveJavaObject(arguments[0].get());
    }
    this.property = ((StringObjectInspector)argumentOIs[1]).getPrimitiveJavaObject(arguments[1].get());

    if (this.property != null) {
      this.property = this.property.toUpperCase();
    }

    if (ls ==null){
      if (argumentOIs.length == 3){
        this.database = ((StringObjectInspector)argumentOIs[1]).getPrimitiveJavaObject(arguments[2].get());
        File f = new File(database);
        if (!f.exists()){
          throw new HiveException(database+" does not exist");
        }
        try {
          ls = new LookupService ( f , LookupService.GEOIP_MEMORY_CACHE );
        } catch (IOException ex){
          throw new HiveException (ex);
        }
      }
      /** // how to do this???
      if (argumentOIs.length == 2) {
        URL u = getClass().getClassLoader().getResource("GeoIP.dat");
        try {
          System.out.println("f exists ?"+ new File(u.getFile()).exists() );
          ls = new LookupService ( u.getFile() );
        } catch (IOException ex){ throw new HiveException (ex); }
      }
       * */
    } // ls null ?

    if (COUNTRY_PROPERTIES.contains(this.property)) {
      Country country = ipString != null ? ls.getCountry(ipString) : ls.getCountry(ipLong);
      if (country == null) {
        return null;
      } else if (this.property.equals(COUNTRY_NAME)) {
        return country.getName();
      } else if (this.property.equals(COUNTRY_CODE)) {
        return country.getCode();
      }
      assert(false);
    } else if (LOCATION_PROPERTIES.contains(this.property)) {
      Location loc = ipString != null ? ls.getLocation(ipString) : ls.getLocation(ipLong);
      if (loc == null) {
        return null;
      }
      //country
      if (this.property.equals(AREA_CODE)) {
        return loc.area_code + "";
      } else if (this.property.equals(CITY)) {
        return loc.city == null ? null : loc.city + "";
      } else if (this.property.equals(DMA_CODE)) {
        return loc.dma_code + "";
      } else if (this.property.equals(LATITUDE)) {
        return loc.latitude + "";
      } else if (this.property.equals(LONGITUDE)) {
	return loc.longitude  + "";
      } else if (this.property.equals(METRO_CODE)) {
        return loc.metro_code + "";
      } else if (this.property.equals(POSTAL_CODE)) {
        return loc.postalCode == null ? null : loc.postalCode + "";
      } else if (this.property.equals(REGION)) {
	return loc.region == null ? null : loc.region + "";
      }
      assert(false);
    } else if (this.property.equals(ORG)) {
      return ipString != null ? ls.getOrg(ipString) : ls.getOrg(ipLong);
    } else if (this.property.equals(ID)) {
      return ipString != null ? ls.getID(ipString) : ls.getID(ipLong);
    }

    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert(children.length == 3);
    return "GenericUDFGeoIP ( "+children[0]+", "+children[1]+", "+children[2]+" )";
  }
}
