package org.apache.carbondata.dictionary

import java.util

import org.apache.spark.sql.Row

import org.apache.carbondata.cardinality.CardinalityMatrix
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl
import org.apache.carbondata.core.writer.sortindex.{CarbonDictionarySortIndexWriterImpl,
CarbonDictionarySortInfoPreparator}

trait GlobalDictionaryUtil {

  /**
   * This method generates the dictionary, dictionarymeta and sortindex files for the input data
   * @param carbonTable
   * @param cardinalityMatrix
   * @param absoluteTableIdentifier
   */
  def writeDictionary(carbonTable: CarbonTable,
      cardinalityMatrix: List[CardinalityMatrix],
      absoluteTableIdentifier: AbsoluteTableIdentifier) = {

    val dimensions: util.List[CarbonDimension] = carbonTable
      .getDimensionByTableName(carbonTable.getFactTableName)
    val measures = carbonTable.getMeasureByTableName(carbonTable.getFactTableName)
    val dimArrSet: Array[Set[String]] = new Array[Set[String]](dimensions.size())
    var index = 0
    cardinalityMatrix.map { cardMatrix =>
      if (isDictionaryColumn(cardMatrix.cardinality)) {
        dimArrSet(index) = Set[String]()
        cardMatrix.columnDataframe.distinct.collect().map { (elem: Row) =>
          val data: String = elem.get(0).toString
          dimArrSet(index)+= (data)
        }
        index += 1
      }
    }
    writeDictionaryToFile(absoluteTableIdentifier, dimArrSet, dimensions)
  }

  /**
   * This method generates the .dict files for the data
   * @param absoluteTableIdentifier
   * @param dimArrSet
   * @param dimensions
   */
 private def writeDictionaryToFile(absoluteTableIdentifier: AbsoluteTableIdentifier,
      dimArrSet: Array[Set[String]],
      dimensions: util.List[CarbonDimension]): Unit = {
    val dictCache: Cache[java.lang.Object, Dictionary] = CacheProvider.getInstance()
      .createCache(CacheType.REVERSE_DICTIONARY, absoluteTableIdentifier.getStorePath)
    var i = 0
    dimArrSet.map { dimSet =>
      val columnIdentifier = new ColumnIdentifier(dimensions.get(i).getColumnId, null, null)
      val writer = new CarbonDictionaryWriterImpl(absoluteTableIdentifier.getStorePath,
        absoluteTableIdentifier.getCarbonTableIdentifier,
        columnIdentifier)

      dimSet.foreach(elem => writer.write(elem))
      writer.close()
      writer.commit()

      val dict: Dictionary = dictCache
        .get(new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier.getCarbonTableIdentifier,
          columnIdentifier, dimensions.get(i).getDataType))
      val newDistinctValues = new util.ArrayList[String]
      val dictionarySortInfoPreparator = new CarbonDictionarySortInfoPreparator()
      val carbonDictionarySortInfo = dictionarySortInfoPreparator
        .getDictionarySortInfo(newDistinctValues, dict, dimensions.get(i).getDataType)

      val carbonDictionarySortIndexWriter = new CarbonDictionarySortIndexWriterImpl(
        absoluteTableIdentifier.getCarbonTableIdentifier,
        columnIdentifier,
        absoluteTableIdentifier.getStorePath)
      i += 1
      try {
        carbonDictionarySortIndexWriter.writeSortIndex(carbonDictionarySortInfo.getSortIndex)
        carbonDictionarySortIndexWriter
          .writeInvertedSortIndex(carbonDictionarySortInfo.getSortIndexInverted)
      } finally {
        carbonDictionarySortIndexWriter.close()
      }
    }
  }

  private def isDictionaryColumn(cardinality: Double): Boolean = {
    val cardinalityThreshold = 0.8
    if (cardinality > cardinalityThreshold) {
      false
    } else {
      true
    }
  }
}

object GlobalDictionaryUtil extends GlobalDictionaryUtil
