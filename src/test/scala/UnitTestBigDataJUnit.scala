import org.junit._
import org.junit.Assert._
class UnitTestBigDataJUnit {

@Test //annotation qui indique c'est un test
  def divionentier()={
  val val_actuel=helloworld.divisionentier(10,2)
  val prevue:Int=5
  assertEquals("le resultat de la diviosn doit etre 5",prevue,val_actuel.toInt)
}
 @Test
  @Before
  def hello(): Unit ={
   helloworld.hello()
 }



}
