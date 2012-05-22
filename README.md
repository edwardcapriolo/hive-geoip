hive-geoip
==========

GeoIP Functions for hive

    add file GeoIP.dat;
    add jar geo-ip-java.jar;
    add jar hive-udf-geo-ip-jtg.jar;
    create temporary function geoip as 'com.jointhegrid.hive.udf.GenericUDFGeoIP';
    select geoip(first, 'COUNTRY_NAME',  './GeoIP.dat' ) from a;

You need a geoip database extracted(separately licenced) found here:
http://geolite.maxmind.com/download/geoip/database/GeoLiteCountry/GeoIP.dat.gz
