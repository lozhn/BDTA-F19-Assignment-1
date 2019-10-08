object implicits {

  implicit class stringUtils(s: String) {
    def sanitizeTrimLower: String = s.replaceAll("""('s)|([\p{Punct}&&[^-]])""", " ").trim.toLowerCase

    def tokenize: Array[String] = s.split("\\s").map(_.sanitizeTrimLower).filter(_.length > 1)
  }
}
