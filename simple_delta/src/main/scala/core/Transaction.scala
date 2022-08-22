package core

sealed trait Transaction {}

case class AddTransaction(val rows: List[Map[String, Any]]) extends Transaction {}

case class DeleteTransaction(val rows: List[Map[String, Any]]) extends Transaction {}