# Datavalidation
pyspark data validation and ingestion


Data validation condition :
1. Quantity sold should not be null
2. vendor id should not be null

Valid data :
Filter the valid records.
validLandingData = refreshedLandingData.filter(psf.col("Quantity_Sold").isNotNull() & psf.col("Vendor_ID").isNotNull())

Invalid data :
Filter the invalid records.
inValidLandingData = refreshedLandingData.filter(psf.col("Quantity_Sold").isNull() | psf.col("Vendor_ID").isNull() |
                                                 psf.col("Sale_Currency").isNull())\
    .withColumn("Hold_Reason", psf
                .when(psf.col("Quantity_Sold").isNull(), "Qty Sold Missing")
                .otherwise(psf.when(psf.col("Vendor_ID").isNull(), "Vendor ID Missing")))
              
To write invalid records in separate file.

inValidLandingData.write\
    .mode("overwrite")\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputLocation + "Hold/HoldData"+currDayZoneSuffix)
