package org.apache.spark.ml.feature.minhash

import org.apache.log4j.LogManager
import org.apache.spark.util.AccumulatorV2

import scala.collection.concurrent.TrieMap

class ClusteringsMapAccumulator(initialValue: TrieMap[String, Int], name: String)
  extends AccumulatorV2[(String, String), TrieMap[String, Int]]{

  //IN is (string, string) OUT is string->(id)
  //input pair are neighbours already
  // ("d_0,d_1", "d_0,d_1") --> clusterid
  private var _map: TrieMap[String, Int] = initialValue
  private var _name: String = name

  def this()={
    this(TrieMap().empty, "")
  }

  override def isZero: Boolean = _map.isEmpty

  override def copy(): AccumulatorV2[(String, String), TrieMap[String, Int]] = {
    val newMap = new ClusteringsMapAccumulator(TrieMap().empty, "")
    newMap._map = this._map
    newMap._name = this._name
    newMap
  }

  override def reset(): Unit = {
    this._map = TrieMap().empty
    this._name = ""
  }

  override def add(v: (String, String)): Unit = {
    val log = LogManager.getRootLogger
    //log.warn("rida="+v._1+", ridb="+v._2)
    //log.warn("before _map="+_map.toString() )
    if ( !_map.contains(v._1) && !_map.contains(v._2) ){
      var newid = if ( isZero ) 1 else _map.values.max + 1
      _map.put(v._1, newid)
      _map.put(v._2, newid)
    }
    if ( _map.contains(v._1) && !_map.contains(v._2) ){
      _map.put(v._2, _map.apply(v._1))
    }
    if ( !_map.contains(v._1) && _map.contains(v._2) ){
      _map.put(v._1, _map.apply(v._2))
    }
    if ( _map.contains(v._1) && _map.contains(v._2) ){
      var id1 = _map.apply(v._1)
      var id2 = _map.apply(v._2)
      if ( id1 != id2 ){
        var newid = if ( isZero ) 1 else _map.values.max + 1
        _map.keys.filter{case key=> _map.apply(key) == id1 || _map.apply(key) == id2 }
          .foreach(key=>{
            _map.put(key, newid)//overwrite old id
          })
      }
    }
    //log.warn("after _map="+_map.toString() )
  }

  override def merge(other: AccumulatorV2[(String, String), TrieMap[String, Int]]): Unit = {
    val log = LogManager.getRootLogger
    var otherMap = other.value
    //log.warn("MERGE")
    //log.warn("_map="+_map.toString() )
    //log.warn("otherMap="+otherMap.toString() )
    if ( _map.isEmpty && !otherMap.isEmpty ){
      _map = otherMap
    }else if ( _map.isEmpty && otherMap.isEmpty ){

    }else if ( !_map.isEmpty && otherMap.isEmpty ){

    }else if ( !_map.isEmpty && !otherMap.isEmpty ){
      for ((inKey, id) <- otherMap) {
        if ( !_map.contains(inKey)) {
          var newid = if ( isZero ) 1 else _map.values.max + 1
          _map.put(inKey, newid)
        }else{
          otherMap.keys.filter{case otherKey=> otherMap.apply(otherKey)==id}
            .foreach(otherKeyMatch=>{
              _map.put(otherKeyMatch, _map.apply(inKey))
            })
        }
      }
    }
    //log.warn("new _map="+_map.toString() )
  }

  override def value: TrieMap[String, Int] = _map
}
