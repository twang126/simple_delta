package core

class DeltaEngine(val log: DeltaLog) {
  def addRow(row: Map[String, Any]): DeltaEngine = {
    DeltaEngine(log.commitTransaction(AddTransaction(List(row))))
  }

  def removeRow(row: Map[String, Any]): DeltaEngine = {
    DeltaEngine(log.commitTransaction(DeleteTransaction(List(row))))
  }
}
