package core

import scala.collection.mutable

class DeltaLog(
                implicit private val ctxt: DeltaContext,
                fileState: Map[String, Any] = Map.empty,
                transactions: Map[String, Transaction] = Map.empty
              ) {
  private val LATEST_CHECKPOINT_FILE_NAME: String = "_latest_checkpoint"
  private val DEFAULT_LATEST_CHECKPOINT_FILE_VERSION: Int = 0
  private val DELTA_CHECKPOINT_FILENAME_PREFIX: String = "checkpoint_"


  def _getLatestCheckpointVersion: Int = {
    fileState.getOrElse(
      LATEST_CHECKPOINT_FILE_NAME, DEFAULT_LATEST_CHECKPOINT_FILE_VERSION
    ).asInstanceOf[Int]
  }

  def _buildTransactionFilePath(version: Int): String = {
    "%10d".format(version)
  }

  def _buildCheckpointFilePath(version: Int): String = {
    f"$DELTA_CHECKPOINT_FILENAME_PREFIX$version"
  }

  def _getLatestTableVersion: (Int, List[Transaction]) = {
    var version = _getLatestCheckpointVersion
    var txns = new mutable.ArrayDeque[Transaction](0)

    while (transactions.contains(_buildTransactionFilePath(version))) {
      version += 1
      val t = transactions(_buildTransactionFilePath(version))
      txns.addOne(t)
    }

    (version, txns.toList)
  }

  def _loadLatestCheckpointFile: DeltaTable = {
    val checkpointVersion = _getLatestCheckpointVersion
    val checkpointFilePath = _buildCheckpointFilePath(checkpointVersion)
    val parsedTable: List[Map[String, Any]] = fileState.getOrElse(
      checkpointFilePath, List.empty).asInstanceOf[List[Map[String, Any]]]

    DeltaTable(
      parsedTable, checkpointVersion
    )
  }

  def _applyTransactions(checkpointDeltaTable: DeltaTable, transactions: List[Transaction], maxVersionId: Int): DeltaTable = {
    var startingTable = checkpointDeltaTable.table

    for (txt <- transactions) {
      txt match {
        case AddTransaction(rows) => startingTable = rows ::: startingTable
        case DeleteTransaction(rows) => {
          startingTable = startingTable.filter(m => !rows.contains(m))
        }
      }
    }

    DeltaTable(startingTable, maxVersionId)
  }

  def getTable: DeltaTable = {
    val checkpointTable = _loadLatestCheckpointFile
    val (txnVersion, transactions) = _getLatestTableVersion

    _applyTransactions(checkpointTable, transactions, txnVersion)
  }
}
