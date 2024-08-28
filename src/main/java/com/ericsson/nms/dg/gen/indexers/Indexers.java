package com.ericsson.nms.dg.gen.indexers;

import com.ericsson.nms.dg.schema.DataTypeType;

/**
 *
 */
public class Indexers {

  /**
   * Get an Indexer class for a data type
   *
   * @param indexerId The ID the indexer will use
   * @param dataType  The data type the indexer will handle
   * @return Indxer instance for the data type. Will handle cardinality and all that....
   */
  public static BaseDataIndexer getIndexer(final String indexerId, final DataTypeType dataType) {
    switch (dataType.getType().toLowerCase()) {
      case "int":
        return new IntegerIndexer(indexerId, dataType);
      case "long":
        return new LongIndexer(indexerId, dataType);
      case "varchar":
//        return new VarcharIndexer_1(indexerId, dataType);
        return new VarcharIndexer(indexerId, dataType);
      case "datetime":
        return new DatetimeIndexer(indexerId, dataType);
      default:
        throw new UnsupportedOperationException(dataType.getType());
    }
  }
}
