import org.apache.spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, IntegerType, Metadata, StringType, StructField, StructType}
class schemadf {


  var exple_schema=StructType(Array(
    StructField("Zipcode",IntegerType,true),
    StructField("ZipCodeType",StringType,true),
    StructField("City",StringType),
    StructField("State",StringType,true)

  ))


}
