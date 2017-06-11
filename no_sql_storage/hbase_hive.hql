use yelp;
create external table business_sample_attributes(
	businessId string, 
	takes_reservations string,
	caters string,
	noise_level string,
	ambience__upscale string,
	accepts_credit_cards string,
	alcohol string,
	good_for__latenight string,
	price_range string
) 
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties ("hbase.columns.mapping" = ":key,att:takes_reservations,att:caters,att:noise_level,att:ambience__upscale,att:accepts_credit_cards,att:alcohol,att:good_for__latenight,att:price_range")
tblproperties ("hbase.table.name" = "business");

