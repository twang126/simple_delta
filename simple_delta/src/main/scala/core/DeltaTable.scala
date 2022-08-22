package core

case class DeltaTable(val table: List[Map[String, Any]], val version: Int)
