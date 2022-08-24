package core

import scala.collection.mutable

class DeltaLog(
                val fileState: Map[String, Any] = Map.empty,
                val transactions: Map[String, Transaction] = Map.empty
              ) {
  private val LATEST_CHECKPOINT_FILE_NAME: String = "_latest_checkpoint"
  private val DEFAULT_LATEST_CHECKPOINT_FILE_VERSION: Int = 0
  private val DELTA_CHECKPOINT_FILENAME_PREFIX: String = "checkpoint_"
  private val DEFAULT_CHECKPOINT_INTERVAL: Int = 10


  def _getLatestCheckpointVersion: Int = {
    fileState.getOrElse(
      LATEST_CHECKPOINT_FILE_NAME, DEFAULT_LATEST_CHECKPOINT_FILE_VERSION
    ).asInstanceOf[Int]
  }

  def _buildTransactionFilePath(version: Int): String = {
    version.toString.reverse.padTo(10, '0').reverse
  }

  def _buildCheckpointFilePath(version: Int): String = {
    f"$DELTA_CHECKPOINT_FILENAME_PREFIX$version"
  }

  def _getLatestTableVersion: (Int, List[Transaction]) = {
    var version = _getLatestCheckpointVersion + 1
    val txns = new mutable.ArrayDeque[Transaction](0)

    while (transactions.contains(_buildTransactionFilePath(version))) {
      val t = transactions(_buildTransactionFilePath(version))
      txns.addOne(t)
      version += 1
    }

    (version - 1, txns.toList)
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

  def commitTransaction(transaction: Transaction): DeltaLog = {
    val optimisticTable = getTable
    val newVersion = optimisticTable.version + 1
    val filepath = _buildTransactionFilePath(newVersion)

    if (newVersion % DEFAULT_CHECKPOINT_INTERVAL != 0) {
      DeltaLog(this.fileState, this.transactions + (filepath -> transaction))
    } else {
      val checkpointPath = _buildCheckpointFilePath(newVersion)
      val newTransactions = this.transactions + (filepath -> transaction)
      transaction match {
        case AddTransaction(rows) => {
          val newRows = rows ::: optimisticTable.table
          DeltaLog(
            this.fileState + (checkpointPath -> newRows) + (LATEST_CHECKPOINT_FILE_NAME -> newVersion),
            newTransactions
          )
        }
        case DeleteTransaction(rows) => {
          DeltaLog(
            this.fileState + (checkpointPath -> optimisticTable.table.filter(m => !rows.contains(m))) + (LATEST_CHECKPOINT_FILE_NAME -> newVersion),
            newTransactions
          )
        }
      }
    }
  }
}
