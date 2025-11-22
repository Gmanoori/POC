object BaseConverter extends App {

  /*  1. Mod by 84 & get remainder
      2. Encode
        - Using .toChar
        - Using predefined List with .charAt
      3. Reassign main number & loop
   */
  def baseConverter1(inp: Long, base: Int): String = {
    val baseChars = (
      ('0' to '9') ++
        ('A' to 'Z') ++
        ('a' to 'z') ++
        List('!', '#', '$', '%', '&', '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<', '=', '>', '?', '@', '[', ']', '^', '_', '{', '|', '}', '~')
      ).mkString

    val alphabet = baseChars.take(base)
    if (inp == 0) return alphabet.charAt(0).toString

    def loop(n: Long): String =
      if (n == 0) ""
      else loop(n / base) + alphabet.charAt((n % base).toInt)

    loop(inp)
  }
  /* Approach 1:
   Pros:
      - Works for dynamic base input
      - Is tailrec (I think)
    Cons:
      - Manual list creation, but one time only
      - Might be hard to understand
   */

  def baseConverter2(inp: Long): String = {
    val sampleList = (48 to 57) ++ (65 to 90) ++ (97 to 122)
    val base = 84
    if (inp == 0) sampleList(0).toChar.toString
    else {
      val rem = inp % base
      val encoded = (sampleList(0)+rem).toChar
      baseConverter2(inp / base) + encoded
    }
  }
  /* Approach 2:
    Pros:
      - Simple, understandable logic & flow
      - Only for base 84, so no confusion
    Cons:
      - Only for base 84 xP hehe
   */

//  println(baseConverter1(15976523425L, 84))
  println(baseConverter2(15976523425L))

//  val sampleList = (48 to 57) ++ (65 to 90) ++ (97 to 122)
//  val finalList = sampleList.map(_.toChar).mkString("")
//  finalList.map(println(_))
}
