import java.sql.{SQLException, Connection, DriverManager, ResultSet, Statement}
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}
import scala.language.postfixOps
import scala.util.matching.Regex
import java.sql.ResultSetMetaData


object HiveWorker extends UserSettings{

  def main(args: Array[String]): Unit = {

    // temp hardcode table to be used
    val tableName = "user_info"


    // select * query
    val sql = "select * from " + tableName;
    val res = stmt.executeQuery(sql);
    //println(res)

    //printReturnData(res)


    def getColumnNames(resultSet: ResultSet): List[String] = {
      // get column names
      val rsmd = resultSet.getMetaData
      val columnCount = rsmd.getColumnCount

      val columnNames = {
        var columnNameList = ArrayBuffer.fill[String](columnCount)("DEFAULT")
        for (x <- 0 to columnCount - 1) {
          columnNameList(x) = rsmd.getColumnName(x + 1)
        }
        columnNameList.toList
      }
      println(columnNames)
      columnNames
    }

    def printReturnData(resultSet: ResultSet): Unit = {

      var rows = Seq[Object]()
      val columnNames: List[String] = getColumnNames(resultSet)
      println(columnNames)
      println("________________________________")

      while (resultSet.next()) {
        var row = Seq[String]()
        for (x <- 1 to resultSet.getMetaData.getColumnCount) {
          var dataRow = "NULL"
          if (resultSet.getString(x) != null) {
            dataRow = resultSet.getString(x)
          }
          var newRowData = dataRow
          row = row :+ newRowData

        }
        rows = rows :+ row
      }
      rows.foreach { r => println(r) }


    }

    def verifyConnection(stmt:Statement, con:Connection ): Boolean = {
      try {
        val stmt = con.createStatement()
        stmt.executeQuery("SELECT 1")
        true
      } catch {
        case e: Exception => false
      }
    }


  }

}

