//utilisation de Flatspectest pour realiser les tests unitaires
import org.apache.hadoop.yarn.webapp.example.HelloWorld
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
class FlatSpecTests extends AnyFlatSpec with Matchers {

  "La divison " should "renvoyer 5" in{
    assert(helloworld.divisionentier(10,2)===5)
  }

  it should ("Send bound of index ") in {
    var list_fruits : List[String] = List("banane","pamplemouse","ananas")
    assertThrows[IndexOutOfBoundsException](list_fruits(4))
  }
   it should("return starting character in a string") in{
     var chaine: String="chaine de charcaters "
     chaine should startWith("c")
   }

}
