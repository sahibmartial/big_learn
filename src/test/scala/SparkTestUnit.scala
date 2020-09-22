//Ici on va tester nos applications Spark avec flatSpec
import org.apache.hadoop.yarn.webapp.example.HelloWorld
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.types.StructType
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
import sparkbigdata.ss
import com.holdenkarau.spark.testing._
//Creatin de session pour nos jeux de donnes en FlatSpec on utilise le mot trait pour nos tests
trait sparksessionprovider {
  val sst = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()
}

class SparkTestUnit extends  AnyFlatSpec with sparksessionprovider with DataFrameSuiteBase {

 it should("initiate spark session")in {
   var env:Boolean=true
   val ss=sparkbigdata.Session_spark(env)
 }

  it should("compare two dataFrame") in{
    val struct_df=List(
      StructField("Employee",StringType,true),
    StructField("Salaire",IntegerType,true))

    val data_df=Seq(
      Row("Sahib",20000),
      Row("Alyx",15000),
      Row("kb",35000)
    )
    //ici on appel les datas manuel d'ou le parallelise

    val df_source : DataFrame = sst.createDataFrame(
      sst.sparkContext.parallelize(data_df),
      StructType(struct_df)
    )
    df_source.show()
    //manipulation de df_source pour df_new onutilise lit pour operer des acion sur colonnes
    val df_new: DataFrame = df_source.withColumn("Salaire",lit(20000))
    df_new.show()

    //compare les colonnnes
  //  assert(df_source.columns.size === df_new.columns.size)
   // assert(df_source.count()===3)//count nbr de ligne
    assert(df_source.take(3).length===3) //similaire a show mais le sdatad depuis le noud ppal an epas utilser

  }




}
