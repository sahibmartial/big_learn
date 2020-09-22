package packageTest.PBGS

class ClassTest {
  def compte_caracte(carac: String) : Int={
   
    if (carac.isEmpty){
      0
    }else{
      carac.trim.length
    }
  }
}
