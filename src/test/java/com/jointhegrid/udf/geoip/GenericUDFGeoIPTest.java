/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jointhegrid.udf.geoip;

import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import java.util.List;
import java.util.Arrays;
import java.io.IOException;
import com.jointhegrid.hive_test.HiveTestService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class GenericUDFGeoIPTest extends HiveTestService {

  public GenericUDFGeoIPTest() throws IOException {
    super();
  }

   public void testCollect() throws Exception {
    Path p = new Path(this.ROOT_DIR, "rankfile");

    FSDataOutputStream o = this.getFileSystem().create(p);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(o));
    bw.write("209.191.139.200\n");
    bw.write("twelve\n");
    bw.close();

    String jarFile;
    jarFile = GenericUDFGeoIP.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar " + jarFile);
    jarFile = com.maxmind.geoip.LookupService.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar " + jarFile);
    //download this or put in reasources
    client.execute(" add file /tmp/GeoIP.dat");

    client.execute("create temporary function geoip as 'com.jointhegrid.udf.geoip.GenericUDFGeoIP'");
    client.execute("create table  ips  ( ip string) row format delimited fields terminated by '09' lines terminated by '10'");
    client.execute("load data local inpath '" + p.toString() + "' into table ips");

    client.execute("select geoip(ip, 'COUNTRY_NAME', './GeoIP.dat') FROM ips");
    List<String> expected = Arrays.asList("United States","N/A");
    assertEquals(expected, client.fetchAll());


    client.execute("drop table ips");
  }
}
