package preprocess

import org.apache.spark.util.AccumulatorV2
import scala.collection.concurrent.TrieMap

class NomLexStatsDimAccu(initialValue: TrieMap[String, TrieMap[String, (Int, Int)]], name: String)
extends AccumulatorV2[(String, String), TrieMap[String, TrieMap[String, (Int, Int)]]]{

  //keys are of form: "lex_d_x->[word_w->(word_id-coord, counts)]"
  private var _lexToWordIdCnt: TrieMap[String, TrieMap[String, (Int, Int)]] = initialValue /////new ParTrieMap[]()
  //cell coord int starts from 0,....
  private var _name: String = name

  def this()={
    this(TrieMap().empty, "")
  }

  override def isZero: Boolean = _lexToWordIdCnt.isEmpty

  override def copy(): AccumulatorV2[(String, String), TrieMap[String, TrieMap[String, (Int, Int)]]] = {
    val newMap = new NomLexStatsDimAccu(TrieMap().empty, "")
    newMap._lexToWordIdCnt = this._lexToWordIdCnt
    newMap._name = this._name
    newMap
  }

  override def reset(): Unit = {
    this._lexToWordIdCnt = TrieMap().empty
    this._name = ""
  }

  override def add(v: (String, String)): Unit = {
    if ( _lexToWordIdCnt.contains(v._1) ){//lex found
      if ( !_lexToWordIdCnt.apply(v._1).contains(v._2) ){//word in lex not found
        _lexToWordIdCnt.apply(v._1).put(v._2, (_lexToWordIdCnt.apply(v._1).values.size, 1) )//next word in lex
      }else{  //word in lex found
        val id=_lexToWordIdCnt.apply(v._1).apply(v._2)._1
        val newcount = _lexToWordIdCnt.apply(v._1).apply(v._2)._2 + 1
        _lexToWordIdCnt.apply(v._1).put(v._2, (id, newcount) )//another one word in lex
      }
    }else{//lex not found
      _lexToWordIdCnt.put(v._1, TrieMap().empty )//new lex, no words
      _lexToWordIdCnt.apply(v._1).put(v._2, (0, 1))//first word in lex (coord-id 0, count 1)
    }
  }

  def lexToWordId: TrieMap[String, TrieMap[String, (Int, Int)]] = _lexToWordIdCnt

  override def merge(other: AccumulatorV2[(String, String), TrieMap[String, TrieMap[String, (Int, Int)]]]): Unit =  other match {
    //for each key of the other, if word exists in this then null, if not add it
    case o: NomLexStatsDimAccu =>
      for ((lex, wordMap) <- o.lexToWordId) {
        if ( _lexToWordIdCnt.contains(lex) ) { //lex found
          for ((word, (id, count)) <- wordMap) {
            if ( !_lexToWordIdCnt.apply(lex).contains(word) ) { //word in lex not found
              _lexToWordIdCnt.apply(lex).put( word, (_lexToWordIdCnt.apply(lex).values.size, count) )//next word in lex
            }else{//word in lex found
              val old_id=_lexToWordIdCnt.apply(lex).apply(word)._1
              val newcount = _lexToWordIdCnt.apply(lex).apply(word)._2 + count
              _lexToWordIdCnt.apply(lex).put(word, (old_id, newcount) )//another one word in lex
            }
          }
        }else{//lex not found
          _lexToWordIdCnt.put(lex, wordMap ) //new lex along with wordMap
        }
      }
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: TrieMap[String, TrieMap[String, (Int, Int)]] = _lexToWordIdCnt
}
