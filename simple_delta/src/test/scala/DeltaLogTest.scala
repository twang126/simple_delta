import core.{AddTransaction, DeleteTransaction, DeltaLog, DeltaTable}
import munit.FunSuite
import org.mockito.Mockito.*
import org.scalatestplus.mockito.MockitoSugar

class DeltaLogTest extends FunSuite with MockitoSugar {
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

    test("DeltaLog.getTableWithDelete") {
      val testLog: DeltaLog = DeltaLog(
        Map(
          "_latest_checkpoint" -> 1, "checkpoint_1" -> List(
            Map("name" -> "Tim", "age" -> 23), Map("name" -> "Eric", "age" -> 20)
          )
        ),
        Map(
          "0000000001" -> AddTransaction(List.empty),
          "0000000002" -> AddTransaction(List(Map("name" -> "Rachel", "age" -> 24))),
          "0000000003" -> DeleteTransaction(List(Map("name" -> "Eric", "age" -> 20)))
        )
      )

      assert(testLog.getTable == DeltaTable(
        List(
          Map("name" -> "Rachel", "age" -> 24), Map("name" -> "Tim", "age" -> 23)
        ),
        3
      ))
    }

    test("DeltaLog.getTableWithNoOpDelete") {
      val testLog: DeltaLog = DeltaLog(
        Map(
          "_latest_checkpoint" -> 1, "checkpoint_1" -> List(
            Map("name" -> "Tim", "age" -> 23), Map("name" -> "Eric", "age" -> 20)
          )
        ),
        Map(
          "0000000001" -> AddTransaction(List.empty),
          "0000000002" -> AddTransaction(List(Map("name" -> "Rachel", "age" -> 24))),
          "0000000003" -> DeleteTransaction(List(Map("name" -> "NonExistent", "age" -> 20)))
        )
      )

      assert(testLog.getTable == DeltaTable(
        List(
          Map("name" -> "Rachel", "age" -> 24), Map("name" -> "Tim", "age" -> 23), Map("name" -> "Eric", "age" -> 20)
        ),
        3
      ))
    }

    test("DeltaLog.commitTransactionNoCheckpoint") {
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

      val resultLog = testLog.commitTransaction(AddTransaction(List(Map("name" -> "Steve", "age" -> 74))))

      // No new checkpoints should have been made
      assert(resultLog.fileState == testLog.fileState)

      // A new transaction should have been added
      assert(resultLog.transactions == Map(
        "0000000001" -> AddTransaction(List.empty),
        "0000000002" -> AddTransaction(List(Map("name" -> "Rachel", "age" -> 24))),
        "0000000003" -> AddTransaction(List(Map("name" -> "Steve", "age" -> 74)))
      ))
    }

    test("DeltaLog.commitTransactionWithCheckpoint") {
      val dl = DeltaLog(
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

      val deltaLogMock = spy(dl)

      when(deltaLogMock.getTable).thenReturn(DeltaTable(
          List(
            Map("name" -> "Rachel", "age" -> 24), Map("name" -> "Tim", "age" -> 23), Map("name" -> "Eric", "age" -> 20)
          ),
          9
      ))

      val updatedDL = deltaLogMock.commitTransaction(AddTransaction(List(Map("name" -> "Roger", "age" -> 45))))

      assert(updatedDL.fileState == Map(
        "_latest_checkpoint" -> 10, "checkpoint_1" -> List(
          Map("name" -> "Tim", "age" -> 23), Map("name" -> "Eric", "age" -> 20)
        ), "checkpoint_10" -> List(
          Map("name" -> "Roger", "age" -> 45), Map("name" -> "Rachel", "age" -> 24), Map("name" -> "Tim", "age" -> 23), Map("name" -> "Eric", "age" -> 20)
        )
      ))

      assert(updatedDL.transactions == Map(
        "0000000001" -> AddTransaction(List.empty),
        "0000000002" -> AddTransaction(List(Map("name" -> "Rachel", "age" -> 24))),
        "0000000010" -> AddTransaction(List(Map("name" -> "Roger", "age" -> 45)))
      ))
    }
}
