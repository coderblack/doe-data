package cn.doitedu.utils

import ch.hsr.geohash.GeoHash

object Functions {

  val gps2GeoHashcode = (lat:Double, lng:Double)=> GeoHash.geoHashStringWithCharacterPrecision(lat,lng,5)

}
