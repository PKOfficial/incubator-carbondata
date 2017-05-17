package org.apache.carbondata.flink;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FlinkRowReadSupportImpl<T> implements CarbonReadSupport<T> {

    private static final long SECONDS_PER_DAY = 60 * 60 * 24L;
    private static final long MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L;
    protected String dateDefaultFormat = "yyyy/MM/dd";

    protected Dictionary[] dictionaries;
    protected DataType[] dataTypes;
    /**
     * carbon columns
     */
    protected CarbonColumn[] carbonColumns;

    @Override
    public void initialize(CarbonColumn[] carbonColumns,
                           AbsoluteTableIdentifier absoluteTableIdentifier) throws IOException {
        this.carbonColumns = carbonColumns;
        dictionaries = new Dictionary[carbonColumns.length];
        dataTypes = new DataType[carbonColumns.length];
        for (int i = 0; i < carbonColumns.length; i++) {
            if (carbonColumns[i].hasEncoding(Encoding.DICTIONARY) && !carbonColumns[i]
                    .hasEncoding(Encoding.DIRECT_DICTIONARY)) {
                CacheProvider cacheProvider = CacheProvider.getInstance();
                Cache<DictionaryColumnUniqueIdentifier, Dictionary> forwardDictionaryCache = cacheProvider
                        .createCache(CacheType.FORWARD_DICTIONARY, absoluteTableIdentifier.getStorePath());
                dataTypes[i] = carbonColumns[i].getDataType();
                dictionaries[i] = forwardDictionaryCache.get(new DictionaryColumnUniqueIdentifier(
                        absoluteTableIdentifier.getCarbonTableIdentifier(),
                        carbonColumns[i].getColumnIdentifier(), dataTypes[i]));
            } else {
                dataTypes[i] = carbonColumns[i].getDataType();
            }
        }
    }

    @Override
    public T readRow(Object[] data) {
        for (int i = 0; i < dictionaries.length; i++) {
            if (dictionaries[i] != null) {
                int key = Integer.parseInt(String.valueOf(data[i]));
                data[i] = dictionaries[i].getDictionaryValueForKey(key);
            } else if (carbonColumns[i].hasEncoding(Encoding.DIRECT_DICTIONARY)) {
                //convert the long to timestamp in case of direct dictionary column
                if (DataType.TIMESTAMP == carbonColumns[i].getDataType()) {
                    if (data[i] != null) {
                        String timestampFormat = CarbonProperties.getInstance().getProperty(
                                CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                                CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
                        long timestamp = Long.parseLong(data[i].toString());
                        Timestamp transformedTimestamp = new Timestamp((timestamp) / 1000);
                        SimpleDateFormat simpleTimestampFormat = new SimpleDateFormat(timestampFormat);
                        data[i] = simpleTimestampFormat.format(transformedTimestamp);
                    }
                } else if (DataType.DATE == carbonColumns[i].getDataType()) {
                    if (data[i] != null) {
                        String dateFormat = CarbonProperties.getInstance().getProperty(
                                CarbonCommonConstants.CARBON_DATE_FORMAT,
                                dateDefaultFormat);
                        Long date = Long.parseLong(String.valueOf(data[i])) * MILLIS_PER_DAY;
                        Date millisTransformedDate = new Date(date);
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
                        data[i] = simpleDateFormat.format(millisTransformedDate);
                    }
                }
            } else {
                data[i] = data[i];
            }
        }
        return (T)data;
    }

    @Override
    public void close() {
        for (Dictionary dictionary : dictionaries) {
            CarbonUtil.clearDictionaryCache(dictionary);
        }
    }

}
