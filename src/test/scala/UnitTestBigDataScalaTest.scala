//test unitaire avec FunSuite
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
class UnitTestBigDataScalaTest extends  AnyFunSuite {
  test("la division doit renvoyer 5") {
    assert(helloworld.divisionentier(10,2)===5)
  }
  test("la Division par zero impossible error arithmetik ") {
    assertThrows[ArithmeticException](helloworld.divisionentier(10,0)===5)
  }
}
