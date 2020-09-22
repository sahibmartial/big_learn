import org.apache.spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, IntegerType, Metadata, StringType, StructField, StructType}
object SchemaDf {

  var exple_schema=StructType(Array(
    StructField("Zipcode",IntegerType,true),
    StructField("ZipCodeType",StringType,true),
    StructField("City",StringType),
    StructField("State",StringType,true)

  ))
  var simpleData = Seq(Row("James ","","Smith","36636","M",3000),
    Row("Michael ","Rose","","40288","M",4000),
    Row("Robert ","","Williams","42114","M",4000),
    Row("Maria ","Anne","Jones","39192","F",4000),
    Row("Jen","Mary","Brown","","F",-1)
  )
  var simpleSchema = StructType(Array(
    StructField("firstname",StringType,true),
    StructField("middlename",StringType,true),
    StructField("lastname",StringType,true),
    StructField("id", StringType, true),
    StructField("gender", StringType, true),
    StructField("salary", IntegerType, true)
  ))


  def schema():StructType={
    var schema=StructType(Array(
      StructField("Zipcode",IntegerType,true),
      StructField("ZipCodeType",StringType,true),
      StructField("City",StringType),
      StructField("State",StringType,true)

    ))
    return schema
  }
}
