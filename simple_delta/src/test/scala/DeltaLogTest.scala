import core.{AddTransaction, DeltaLog, DeltaTable};

class DeltaLogTest extends munit.FunSuite {
    test("DeltaLog._getLatestCheckpointVersion") {
        val testLog: DeltaLog = DeltaLog(
                Map(
                        "_latest_checkpoint" -> 1, "checkpoint_" -> List(
                                    Map("name" -> "Tim", "age" -> 23), Map("name" -> "Eric", "age" -> 20)
                                )
                )
        )

        assert(testLog._getLatestCheckpointVersion == 1)
    }

    test("DeltaLog._loadLatestCheckpointFile") {
      val testLog: DeltaLog = DeltaLog(
        Map(
          "_latest_checkpoint" -> 1, "checkpoint_1" -> List(
            Map("name" -> "Tim", "age" -> 23), Map("name" -> "Eric", "age" -> 20)
          )
        )
      )

      val expectedDeltaTable = DeltaTable(List(
        Map("name" -> "Tim", "age" -> 23), Map("name" -> "Eric", "age" -> 20)
      ), 1)

      assert(testLog._loadLatestCheckpointFile == expectedDeltaTable)
    }

    test("DeltaLog._buildTransactionFilePath") {
      val testLog: DeltaLog = DeltaLog()

      assert(testLog._buildTransactionFilePath(1) == "0000000001")
    }

    test("DeltaLog._getLatestTableVersion_NoTransactionsAfterCheckpoint") {
      val testLog: DeltaLog = DeltaLog(
        Map(
          "_latest_checkpoint" -> 1, "checkpoint_1" -> List(
            Map("name" -> "Tim", "age" -> 23), Map("name" -> "Eric", "age" -> 20)
          )
        ),
        Map(
          "0000000001" -> AddTransaction(List.empty)
        )
      )

      assert(testLog._getLatestTableVersion == (1 -> List.empty))
    }

    test("DeltaLog._getLatestTableVersion") {
      val testLog: DeltaLog = DeltaLog(
        Map(
          "_latest_checkpoint" -> 1, "checkpoint_1" -> List(
            Map("name" -> "Tim", "age" -> 23), Map("name" -> "Eric", "age" -> 20)
          )
        ),
        Map(
          "0000000001" -> AddTransaction(List.empty),
          "0000000002" -> AddTransaction(List(Map("name" -> "Rachel", "age" -> 24)))
        )
      )

      assert(testLog._getLatestTableVersion == (2 -> List(AddTransaction(List(Map("name" -> "Rachel", "age" -> 24))))))
    }

    test("DeltaLog.getTable") {
      val testLog: DeltaLog = DeltaLog(
        Map(
          "_latest_checkpoint" -> 1, "checkpoint_1" -> List(
            Map("name" -> "Tim", "age" -> 23), Map("name" -> "Eric", "age" -> 20)
          )
        ),
        Map(
          "0000000001" -> AddTransaction(List.empty),
          "0000000002" -> AddTransaction(List(Map("name" -> "Rachel", "age" -> 24)))
        )
      )
      
      assert(testLog.getTable == DeltaTable(
        List(
          Map("name" -> "Rachel", "age" -> 24), Map("name" -> "Tim", "age" -> 23), Map("name" -> "Eric", "age" -> 20)
        ),
        2
      ))
    }
}