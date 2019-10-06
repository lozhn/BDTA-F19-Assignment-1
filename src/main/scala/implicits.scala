object implicits {

  implicit class stringUtils(s: String) {
    def sanitizeTrimLower: String = s.replaceAll("""('s)|([\p{Punct}&&[^-]])""", " ").trim.toLowerCase
  }
}
